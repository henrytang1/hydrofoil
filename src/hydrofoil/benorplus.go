package hydrofoil

import (
	"log"
	"math/rand"
	"sort"
	"time"
)

/************************************** Ben Or Braodcast **********************************************/
func (r *Replica) startBenOrPlus() {
	if r.leaderState.isLeader || r.benOrState.benOrRunning {
		return
	}

	r.startNewBenOrPlusIteration(max(1, r.benOrState.benOrIteration), make([]Entry, 0), make([]bool, r.N))
}

func (r *Replica) startNewBenOrPlusIteration(iteration int, benOrBroadcastMsg []Entry, heardServerFromBroadcast []bool) {
	var broadcastEntry Entry
	benOrIndex := r.commitIndex + 1
	clearTimer(r.benOrWaitTimer)
	if benOrIndex < len(r.log) {
		broadcastEntry = r.log[benOrIndex]
		broadcastEntry.Index = int32(r.commitIndex)+1
	} else if !r.pq.isEmpty() {
		broadcastEntry = r.pq.peek()
		broadcastEntry.Index = int32(r.commitIndex)+1
	} else {
		r.sendGetCommittedData()
		// nothing to run ben-or on right now, wait until timeout again
		r.benOrState = emptyBenOrState
		r.benOrState.benOrIteration = iteration
		timeout := rand.Intn(r.benOrStartTimeout/2) + r.benOrStartTimeout/2
		setTimer(r.benOrStartTimer, time.Duration(timeout) * time.Millisecond)

		clearTimer(r.benOrResendTimer)
		return
	}

	heardServerFromBroadcast[r.Id] = true

	r.benOrState = BenOrState{
		benOrRunning: true,

		benOrIteration: iteration,
		benOrPhase: 1,
		benOrStage: Broadcasting,

		benOrBroadcastEntry: broadcastEntry,
		benOrBroadcastMsgs: append(benOrBroadcastMsg, broadcastEntry),
		heardServerFromBroadcast: heardServerFromBroadcast,

		haveMajEntry: false,
		benOrMajEntry: emptyEntry,

		benOrVote: VoteUninitialized,
		benOrConsensusMsgs: make([]uint8, 0),
		heardServerFromConsensus: make([]bool, r.N),

		prevPhaseFinalValue: VoteUninitialized,
	}
	r.startBenOrBroadcast()
	r.maybeAdvanceBenOr()
}

func (r *Replica) startBenOrBroadcast() {
	entries := r.copySuffix(r.commitIndex + 1)

	args := &BenOrBroadcast{
		SenderId: r.Id, Term: int32(r.term), CommitIndex: int32(r.commitIndex), LogTerm: int32(r.logTerm), LogLength: int32(len(r.log)),
		Iteration: int32(r.benOrState.benOrIteration), BroadcastEntry: r.benOrState.benOrBroadcastEntry,
		StartIndex: int32(r.commitIndex) + 1, Entries: entries, PQEntries: r.pq.extractList(),
	}

	timeout := rand.Intn(r.benOrResendTimeout/2) + r.benOrResendTimeout/2
	setTimer(r.benOrResendTimer, time.Duration(timeout) * time.Millisecond)

	for i := 0; i < r.N; i++ {
		if int32(i) != r.Id {
			r.SendMsg(int32(i), r.benOrBroadcastRPC, args)
		}
	}
}

func (r *Replica) updateLogIfPossible(rpc ReplyMsg) bool {
	if r.isLogMoreUpToDate(rpc) == LessUpToDate {
		if r.hasLogData(rpc, r.commitIndex + 1) {
			r.updateLogFromRPC(rpc)
			return true
		} else {
			// Otherwise, we remain out of date. If this is the case, then we wait until we're also running BenOrPlus.
			// When we send a BenOrPlus message and we're out of date, we'll get back data allowing us to update our log.
			return false
		}
	} else {
		r.getNewValues(rpc)
		return true
	}
}

func (r *Replica) handleBenOrBroadcast(rpc BenOrBroadcastMsg) {
	r.handleIncomingTerm(rpc)

	if !r.updateLogIfPossible(rpc) {
		r.sendOneGetCommittedData(int(rpc.GetSenderId()))
		return
	}

	if rpc.GetBenOrMsgValid() == False {
		// A higher iteration ends our current one, even if the sender is not running Ben-Or+. Its broadcast entry doesn't count.
		if r.benOrState.benOrRunning && r.commitIndex == int(rpc.GetCommitIndex()) && r.benOrState.benOrIteration < int(rpc.GetIteration()) {
			r.startNewBenOrPlusIteration(int(rpc.GetIteration()), make([]Entry, 0), make([]bool, r.N))
		}
		return
	}

	// at this point, r.commitIndex >= int(rpc.GetCommitIndex())
	if r.commitIndex == int(rpc.GetCommitIndex()) {
		if r.benOrState.benOrIteration < int(rpc.GetIteration()) {
			heardServerFromBroadcast := make([]bool, r.N)
			heardServerFromBroadcast[rpc.GetSenderId()] = true
			r.startNewBenOrPlusIteration(int(rpc.GetIteration()), []Entry{rpc.GetBroadcastEntry()}, heardServerFromBroadcast)

		} else if r.benOrState.benOrRunning && r.benOrState.benOrIteration == int(rpc.GetIteration()) && r.benOrState.benOrStage == Broadcasting {
			heardFromServerBefore := r.benOrState.heardServerFromBroadcast[rpc.GetSenderId()]

			if !heardFromServerBefore {
				r.benOrState.benOrBroadcastMsgs = append(r.benOrState.benOrBroadcastMsgs, rpc.GetBroadcastEntry())
				r.benOrState.heardServerFromBroadcast[rpc.GetSenderId()] = true

				r.maybeAdvanceBenOr()
			}
		}
	}

	if t, ok := rpc.(*BenOrBroadcast); ok {
		r.sendBenOrReply(rpc.GetSenderId(), int(t.CommitIndex))
	}
	// otherwise, this is a BenOrBroadcastReply, so we don't need to send anything back to the sender.
	return
}

func (r *Replica) getMajorityBroadcastMsg() (bool, Entry) {
	var numMsgs int = len(r.benOrState.benOrBroadcastMsgs)
	msgs := r.benOrState.benOrBroadcastMsgs

	sort.Slice(msgs, func(i, j int) bool {
		if msgs[i].ServerId != msgs[j].ServerId {
			return msgs[i].ServerId < msgs[j].ServerId
		}
		if msgs[i].Timestamp != msgs[j].Timestamp {
			return msgs[i].Timestamp < msgs[j].Timestamp
		}
		return msgs[i].Term < msgs[j].Term
	})

	haveMajEntry := false
	majEntry := emptyEntry

	counter := 0
	senderId := int32(-1)
	timestamp := int64(-1)

	for i := 0; i < numMsgs; i++ {
		if msgs[i].ServerId == senderId && msgs[i].Timestamp == timestamp {
			counter++
		} else {
			counter = 1
			senderId = msgs[i].ServerId
			timestamp = msgs[i].Timestamp
		}

		if counter > r.N/2 {
			haveMajEntry = true
			majEntry = msgs[i]
			break
		}
	}

	return haveMajEntry, majEntry
}

func (r *Replica) initialBenOrVote() uint8 {
	if !r.benOrState.haveMajEntry {
		return Vote0
	}
	if r.commitIndex + 1 < len(r.log) && !entryEqual(r.log[r.commitIndex + 1], r.benOrState.benOrMajEntry) {
		return Vote0
	}
	return Vote1
}

func (r *Replica) benOrCoinFlip() uint8 {
	if r.commitIndex + 1 < len(r.log) {
		return Vote0
	}
	return uint8(rand.Intn(2))
}

/************************************** Ben Or Consensus **********************************************/

func (r *Replica) enterBenOrConsensus(stage uint8, vote uint8) {
	clearTimer(r.benOrWaitTimer)
	r.benOrState.benOrRunning = true
	r.benOrState.benOrStage = stage
	r.benOrState.benOrVote = vote
	r.benOrState.benOrConsensusMsgs = append(make([]uint8, 0), vote)
	r.benOrState.heardServerFromConsensus = make([]bool, r.N)
	r.benOrState.heardServerFromConsensus[r.Id] = true

	r.startBenOrConsensusStage()
}

// only called if actually need to run benOr at this index
func (r *Replica) startBenOrConsensusStage() {
	timeout := rand.Intn(r.benOrResendTimeout/2) + r.benOrResendTimeout/2
	setTimer(r.benOrResendTimer, time.Duration(timeout)*time.Millisecond)

	entries := r.copySuffix(r.commitIndex + 1)

	args := &BenOrConsensus{
		SenderId: r.Id, Term: int32(r.term), CommitIndex: int32(r.commitIndex), LogTerm: int32(r.logTerm), LogLength: int32(len(r.log)),
		Iteration: int32(r.benOrState.benOrIteration), Phase: int32(r.benOrState.benOrPhase), Stage: r.benOrState.benOrStage,
		Vote: r.benOrState.benOrVote, PrevPhaseFinalValue: r.benOrState.prevPhaseFinalValue, HaveMajEntry: convertBoolToInteger(r.benOrState.haveMajEntry), MajEntry: r.benOrState.benOrMajEntry,
		Entries: entries, PQEntries: r.pq.extractList(),
	}

	for i := 0; i < r.N; i++ {
		if int32(i) != r.Id {
			r.SendMsg(int32(i), r.benOrConsensusRPC, args)
		}
	}
}

func (r *Replica) handleBenOrConsensus(rpc BenOrConsensusMsg) {
	r.handleIncomingTerm(rpc)

	if !r.updateLogIfPossible(rpc) {
		r.sendOneGetCommittedData(int(rpc.GetSenderId()))
		return
	}

	if rpc.GetBenOrMsgValid() == False {
		return
	}

	// at this point, r.commitIndex >= int(rpc.GetCommitIndex())
	if r.commitIndex == int(rpc.GetCommitIndex()) {
		caseIteration := r.benOrState.benOrIteration < int(rpc.GetIteration())
		casePhase := r.benOrState.benOrIteration == int(rpc.GetIteration()) && r.benOrState.benOrPhase < int(rpc.GetPhase())
		caseStage := r.benOrState.benOrIteration == int(rpc.GetIteration()) && r.benOrState.benOrPhase == int(rpc.GetPhase()) && (!r.benOrState.benOrRunning || r.benOrState.benOrStage < rpc.GetStage())

		if caseIteration || casePhase || caseStage {
			if caseIteration {
				r.benOrState = emptyBenOrState
			}

			r.benOrState.benOrIteration = int(rpc.GetIteration())
			r.benOrState.benOrPhase = int(rpc.GetPhase())
			r.benOrState.prevPhaseFinalValue = rpc.GetPrevPhaseFinalValue()

			if convertIntegerToBool(rpc.GetHaveMajEntry()) {
				r.benOrState.haveMajEntry = true
				r.benOrState.benOrMajEntry = rpc.GetMajEntry()
			}

			var vote uint8
			if rpc.GetStage() == StageOne {
				if rpc.GetPhase() == 1 {
					vote = r.initialBenOrVote()
				} else {
					if rpc.GetPrevPhaseFinalValue() == VoteQuestionMark {
						vote = r.benOrCoinFlip()
					} else {
						vote = rpc.GetVote()
					}
				}
			} else {
				vote = rpc.GetVote()
			}

			r.enterBenOrConsensus(rpc.GetStage(), vote)
		}

		if r.benOrState.benOrIteration == int(rpc.GetIteration()) && r.benOrState.benOrPhase == int(rpc.GetPhase()) && r.benOrState.benOrStage == rpc.GetStage() {
			haveMajEntry := r.benOrState.haveMajEntry
			majEntry := r.benOrState.benOrMajEntry

			if convertIntegerToBool(rpc.GetHaveMajEntry()) {
				if !haveMajEntry || r.benOrState.benOrMajEntry.Term < rpc.GetMajEntry().Term {
					majEntry = rpc.GetMajEntry()
				}
				haveMajEntry = true
			}

			r.benOrState.haveMajEntry = haveMajEntry
			r.benOrState.benOrMajEntry = majEntry

			heardFromServerBefore := r.benOrState.heardServerFromConsensus[rpc.GetSenderId()]

			if !heardFromServerBefore {
				r.benOrState.benOrConsensusMsgs = append(r.benOrState.benOrConsensusMsgs, rpc.GetVote())
				r.benOrState.heardServerFromConsensus[rpc.GetSenderId()] = true

				r.maybeAdvanceBenOr()
			}
		}
	}

	if t, ok := rpc.(*BenOrConsensus); ok {
		r.sendBenOrReply(rpc.GetSenderId(), int(t.GetCommitIndex()))
	}
	// otherwise, this is a BenOrBroadcastReply, so we don't need to send anything back to the sender.
	return
}

func (r *Replica) maybeAdvanceBenOr() {
	if !r.benOrState.benOrRunning {
		return
	}

	numMsgs := len(r.benOrState.benOrConsensusMsgs)
	if r.benOrState.benOrStage == Broadcasting {
		numMsgs = len(r.benOrState.benOrBroadcastMsgs)
	}

	if numMsgs <= r.N/2 {
		return
	}

	if r.benOrWaitTimeout > 0 {
		if !r.benOrWaitTimer.active {
			setTimer(r.benOrWaitTimer, r.benOrWaitTimeout)
		}
		return
	}

	r.advanceBenOr()
}

func (r *Replica) advanceBenOr() {
	if !r.benOrState.benOrRunning {
		return
	}

	if r.benOrState.benOrStage == Broadcasting {
		r.benOrState.haveMajEntry, r.benOrState.benOrMajEntry = r.getMajorityBroadcastMsg()
		r.enterBenOrConsensus(StageOne, r.initialBenOrVote())
		r.maybeAdvanceBenOr()
		return
	}

	msgs := r.benOrState.benOrConsensusMsgs

	voteCount := [3]int{0, 0}
	for _, msg := range msgs {
		voteCount[msg]++
	}

	if r.benOrState.benOrStage == StageTwo {
		if voteCount[0] > 0 && voteCount[1] > 0 {
			log.Fatal("Should not have both votes")
		}
	}

	if r.benOrState.benOrStage == StageOne {
		var vote uint8;
		if voteCount[0] > r.N/2 {
			vote = Vote0
		} else if voteCount[1] > r.N/2 {
			vote = Vote1
		} else {
			vote = VoteQuestionMark
		}

		r.enterBenOrConsensus(StageTwo, vote)
		r.maybeAdvanceBenOr()
	} else if r.benOrState.benOrStage == StageTwo {
		if voteCount[0] > r.N/2 {
			if r.leaderState.isLeader {
				iteration := r.benOrState.benOrIteration + 1
				r.resetBenOr()
				r.benOrState.benOrIteration = iteration
			} else {
				r.startNewBenOrPlusIteration(r.benOrState.benOrIteration + 1, make([]Entry, 0), make([]bool, r.N))
			}
		} else if voteCount[1] > r.N/2 {
			// commit entry
			if !r.benOrState.haveMajEntry {
				log.Fatal("Ben-Or+ decision of 1 without a majority entry")
			}

			newCommittedEntry := r.benOrState.benOrMajEntry
			benOrIndex := r.commitIndex + 1
			newCommittedEntry.Index = int32(benOrIndex)
			newCommittedEntry.Term, newCommittedEntry.Version = -1, 0 // null stamp
			potentialEntries := make([]Entry, 0)
			rewritten := false
			appended := false

			if benOrIndex < len(r.log) {
				if !entryEqual(r.log[benOrIndex], newCommittedEntry) {
					for i := benOrIndex; i < len(r.log); i++ {
						r.inLog.remove(r.log[i])
						potentialEntries = append(potentialEntries, r.log[i])
					}
					r.log = append([]Entry(nil), r.log[:benOrIndex]...) // preserve the entries in messages already sent

					r.inLog.add(newCommittedEntry)
					r.log = append(r.log, newCommittedEntry)
					r.pq.remove(newCommittedEntry)
					rewritten = true
				}
			} else {
				r.inLog.add(newCommittedEntry)
				r.log = append(r.log, newCommittedEntry)
				r.pq.remove(newCommittedEntry)
				appended = true
			}

			if rewritten {
				r.cancelCampaign()
			}

			for _, v := range(potentialEntries) {
				if !r.seenBefore(v) { r.pq.push(v) }
			}

			if r.leaderState.isLeader && (rewritten || appended) {
				if rewritten {
					r.leaderState.version++
				}
				r.appendQueuedEntries()
				r.stampSuffix(benOrIndex)
				r.invalidateReplicationFrom(benOrIndex)
			}

			r.commitIndex++
			r.resetBenOr()

		} else {
			var prevPhaseFinalValue uint8
			var initialVote uint8
			if voteCount[0] > 0 {
				prevPhaseFinalValue = Vote0
				initialVote = Vote0
			} else if voteCount[1] > 0 {
				prevPhaseFinalValue = Vote1
				initialVote = Vote1
			} else {
				prevPhaseFinalValue = VoteQuestionMark
				initialVote = r.benOrCoinFlip()
			}

			r.benOrState.benOrPhase++
			r.benOrState.prevPhaseFinalValue = prevPhaseFinalValue
			r.enterBenOrConsensus(StageOne, initialVote)
			r.maybeAdvanceBenOr()
		}
	}
}

/************************************** BenOr Reply **********************************************/
func (r *Replica) sendBenOrReply(senderId int32, rpcCommitIndex int) {
	entries := r.copySuffix(rpcCommitIndex + 1)

	if r.benOrState.benOrStage == NotRunning || r.benOrState.benOrStage == Broadcasting {
		args := &BenOrBroadcastReply{
			SenderId: r.Id, Term: int32(r.term), CommitIndex: int32(r.commitIndex), LogTerm: int32(r.logTerm), LogLength: int32(len(r.log)),
			BenOrMsgValid: convertBoolToInteger(r.benOrState.benOrRunning), Iteration: int32(r.benOrState.benOrIteration), BroadcastEntry: r.benOrState.benOrBroadcastEntry,
			StartIndex: int32(rpcCommitIndex) + 1, Entries: entries, PQEntries: r.pq.extractList(),
		}
		r.SendMsg(senderId, r.benOrBroadcastReplyRPC, args)

	} else if r.benOrState.benOrStage == StageOne || r.benOrState.benOrStage == StageTwo {
		args := &BenOrConsensusReply{
			SenderId: r.Id, Term: int32(r.term), CommitIndex: int32(r.commitIndex), LogTerm: int32(r.logTerm), LogLength: int32(len(r.log)),
			BenOrMsgValid: True, Iteration: int32(r.benOrState.benOrIteration), Phase: int32(r.benOrState.benOrPhase), Stage: r.benOrState.benOrStage,
			Vote: r.benOrState.benOrVote, PrevPhaseFinalValue: r.benOrState.prevPhaseFinalValue, HaveMajEntry: convertBoolToInteger(r.benOrState.haveMajEntry), MajEntry: r.benOrState.benOrMajEntry,
			StartIndex: int32(rpcCommitIndex) + 1, Entries: entries, PQEntries: r.pq.extractList(),
		}
		r.SendMsg(senderId, r.benOrConsensusReplyRPC, args)
	}
}

/************************************** Resend Ben Or **********************************************/

func (r *Replica) resendBenOrTimer() {
	timeout := rand.Intn(r.benOrResendTimeout/2) + r.benOrResendTimeout/2
	setTimer(r.benOrResendTimer, time.Duration(timeout)*time.Millisecond)

	entries := r.copySuffix(r.commitIndex + 1)

	if !r.benOrState.benOrRunning {
		log.Fatal("Resending BenOr when not running")
	}

	if r.benOrState.benOrStage == Broadcasting {
		args := &BenOrBroadcast{
			SenderId: r.Id, Term: int32(r.term), CommitIndex: int32(r.commitIndex), LogTerm: int32(r.logTerm), LogLength: int32(len(r.log)),
			Iteration: int32(r.benOrState.benOrIteration), BroadcastEntry: r.benOrState.benOrBroadcastEntry,
			StartIndex: int32(r.commitIndex) + 1, Entries: entries, PQEntries: r.pq.extractList(),
		}
		for i := 0; i < r.N; i++ {
			if int32(i) != r.Id {
				r.SendMsg(int32(i), r.benOrBroadcastRPC, args)
			}
		}
	}

	if r.benOrState.benOrStage == StageOne || r.benOrState.benOrStage == StageTwo {
		args := &BenOrConsensus{
			SenderId: r.Id, Term: int32(r.term), CommitIndex: int32(r.commitIndex), LogTerm: int32(r.logTerm), LogLength: int32(len(r.log)),
			Iteration: int32(r.benOrState.benOrIteration), Phase: int32(r.benOrState.benOrPhase), Stage: r.benOrState.benOrStage,
			Vote: r.benOrState.benOrVote, PrevPhaseFinalValue: r.benOrState.prevPhaseFinalValue, HaveMajEntry: convertBoolToInteger(r.benOrState.haveMajEntry), MajEntry: r.benOrState.benOrMajEntry,
			Entries: entries, PQEntries: r.pq.extractList(),
		}
		for i := 0; i < r.N; i++ {
			if int32(i) != r.Id {
				r.SendMsg(int32(i), r.benOrConsensusRPC, args)
			}
		}
	}
}
