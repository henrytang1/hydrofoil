package hydrofoil

import (
	"log"
	"math/rand"
	"time"
)

func (r *Replica) handleReplicateEntries(rpc *ReplicateEntries) {
	r.handleIncomingTerm(rpc)
	if r.term > int(rpc.Term) || r.isLogMoreUpToDate(rpc) == MoreUpToDate {
		if r.term == int(rpc.Term) {
			r.cancelCampaign()

			timeout := rand.Intn(r.electionTimeout/2) + r.electionTimeout/2
			setTimer(r.electionTimer, time.Duration(timeout)*time.Millisecond)

			if !r.benOrState.benOrRunning {
				timeout = rand.Intn(r.benOrStartTimeout/2) + r.benOrStartTimeout/2
				setTimer(r.benOrStartTimer, time.Duration(timeout)*time.Millisecond)
			}

			r.lastHeardFromLeader = time.Now()
		}

		entries := r.copySuffix(int(rpc.CommitIndex) + 1)

		// NewRequestedIndex is not used because the leader will either switch to a follower (because of the term) or ignore this reply (because it has an older LeaderTimestamp)
		args := &ReplicateEntriesReply{
			SenderId: r.Id, Term: int32(r.term), CommitIndex: int32(r.commitIndex), LogTerm: int32(r.logTerm), LogLength: int32(len(r.log)),
			StartIndex: rpc.CommitIndex + 1, Entries: entries, PQEntries: r.pq.extractList(), NewRequestedIndex: int32(r.commitIndex) + 1,
			MsgTimestamp: time.Now().UnixNano(), RequestTerm: rpc.Term, Version: rpc.Version, Success: False,
		}

		r.SendMsg(rpc.SenderId, r.replicateEntriesReplyRPC, args)

		if r.term == int(rpc.Term) {
			timeout := rand.Intn(r.electionTimeout/2) + r.electionTimeout/2
			setTimer(r.electionTimer, time.Duration(timeout)*time.Millisecond)
		}

		return
	}

	// at this point, r.term == rpc.Term
	timeout := rand.Intn(r.electionTimeout/2) + r.electionTimeout/2
	setTimer(r.electionTimer, time.Duration(timeout)*time.Millisecond)

	if r.candidateState.isCandidate {
		r.candidateState = emptyCandidateState
	}

	if !r.benOrState.benOrRunning {
		timeout = rand.Intn(r.benOrStartTimeout/2) + r.benOrStartTimeout/2
		setTimer(r.benOrStartTimer, time.Duration(timeout)*time.Millisecond)
	}

	r.lastHeardFromLeader = time.Now()

	if rpc.PrevLogIndex < 0 || (int(rpc.PrevLogIndex) > r.commitIndex && !r.matchesPreviousEntry(rpc)) || !r.hasLogData(rpc, int(rpc.PrevLogIndex) + 1) {

		entries := r.copySuffix(int(rpc.CommitIndex) + 1)

		// NewRequestedIndex is not used because the leader will either switch to a follower (because of the term) or ignore this reply (because it has an older LeaderTimestamp)
		args := &ReplicateEntriesReply{
			SenderId: r.Id, Term: int32(r.term), CommitIndex: int32(r.commitIndex), LogTerm: int32(r.logTerm), LogLength: int32(len(r.log)),
			StartIndex: rpc.CommitIndex + 1, Entries: entries, PQEntries: r.pq.extractList(), NewRequestedIndex: int32(r.commitIndex) + 1,
			MsgTimestamp: time.Now().UnixNano(), RequestTerm: rpc.Term, Version: rpc.Version, Success: False,
		}

		r.SendMsg(rpc.SenderId, r.replicateEntriesReplyRPC, args)
		return
	}

	if r.leaderState.isLeader {
		log.Fatal("Should not be leader when receiving a ReplicateEntries RPC, and have a greater term!")
	}

	r.updateLog(rpc, int(rpc.PrevLogIndex) + 1)

	entries := r.copySuffix(int(rpc.CommitIndex) + 1)

	if !r.benOrState.benOrRunning || r.benOrState.benOrStage == Broadcasting {
		args := &ReplicateEntriesReply{
			SenderId: r.Id, Term: int32(r.term), CommitIndex: int32(r.commitIndex), LogTerm: int32(r.logTerm), LogLength: int32(len(r.log)),
			StartIndex: rpc.CommitIndex + 1, Entries: entries, PQEntries: r.pq.extractList(), NewRequestedIndex: rpc.LogLength,
			MsgTimestamp: time.Now().UnixNano(), RequestTerm: rpc.Term, Version: rpc.Version, Success: True,
		}

		if rpc.LogLength != rpc.GetStartIndex() + int32(len(rpc.Entries)) {
			log.Fatal("log length and start index + entries length do not match for replica", r.Id, rpc.LogLength, rpc.GetStartIndex(), int32(len(rpc.Entries)))
		}

		r.SendMsg(rpc.SenderId, r.replicateEntriesReplyRPC, args)
	} else {
		args := &ReplicateEntriesReply{
			SenderId: r.Id, Term: int32(r.term), CommitIndex: int32(r.commitIndex), LogTerm: int32(r.logTerm), LogLength: int32(len(r.log)),
			StartIndex: rpc.CommitIndex + 1, Entries: entries, PQEntries: r.pq.extractList(), NewRequestedIndex: int32(r.commitIndex) + 1,
			MsgTimestamp: time.Now().UnixNano(), RequestTerm: rpc.Term, Version: rpc.Version, Success: False,
		}

		r.SendMsg(rpc.SenderId, r.replicateEntriesReplyRPC, args)
	}
}

func (r *Replica) handleReplicateEntriesReply (rpc *ReplicateEntriesReply) {
	r.handleIncomingTerm(rpc)

	if r.term > int(rpc.Term) || !r.leaderState.isLeader || int(rpc.RequestTerm) != r.term {
		return
	}

	if !r.updateLogIfPossible(rpc) {
		r.sendOneGetCommittedData(int(rpc.SenderId))
		return
	}

	// Check the version after updating the log, since the reply's committed entries may have caused a rewrite
	if !r.leaderState.isLeader || int(rpc.Version) != r.leaderState.version {
		return
	}

	if rpc.Success == True {
		r.leaderState.repMatchIndex[rpc.SenderId] = max(r.leaderState.repMatchIndex[rpc.SenderId], int(rpc.NewRequestedIndex) - 1)
		r.leaderState.repNextIndex[rpc.SenderId] = max(r.leaderState.repNextIndex[rpc.SenderId], int(rpc.NewRequestedIndex))
	} else {
		r.leaderState.repNextIndex[rpc.SenderId] = max(r.leaderState.repMatchIndex[rpc.SenderId] + 1, min(r.leaderState.repNextIndex[rpc.SenderId], int(rpc.NewRequestedIndex)))
	}
}

func (r *Replica) matchesPreviousEntry(rpc *ReplicateEntries) bool {
	index := int(rpc.PrevLogIndex)
	if index < 1 || index >= len(r.log) {
		return false
	}
	entry := r.log[index]
	return stampValid(entry) && entry.Term == rpc.PrevLogTerm && entry.Version == rpc.PrevLogVersion
}

/************************************** Replicate Entries **********************************************/

func (r *Replica) broadcastReplicateEntries() {
	for i := 0; i < r.N; i++ {
		if int32(i) != r.Id {
			prevLogIndex := int32(r.leaderState.repNextIndex[i]) - 1

			args := &ReplicateEntries{
				SenderId: r.Id, Term: int32(r.term), CommitIndex: int32(r.commitIndex), LogTerm: int32(r.logTerm), LogLength: int32(len(r.log)),
				PrevLogIndex: prevLogIndex, PrevLogServerId: r.log[prevLogIndex].ServerId, PrevLogTimestamp: r.log[prevLogIndex].Timestamp,
				PrevLogTerm: r.log[prevLogIndex].Term, PrevLogVersion: r.log[prevLogIndex].Version, Version: int32(r.leaderState.version),
				Entries: r.copySuffix(r.leaderState.repNextIndex[i]),
			}

			r.SendMsg(int32(i), r.replicateEntriesRPC, args)
		}
	}
}
