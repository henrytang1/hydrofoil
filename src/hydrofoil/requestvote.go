package hydrofoil

import (
	"fmt"
	"math/rand"
	"time"
)

/************************************** Election **********************************************/

func (r *Replica) startElection() {
	timeout := rand.Intn(r.electionTimeout/2) + r.electionTimeout/2
	setTimer(r.electionTimer, time.Duration(timeout)*time.Millisecond)
	clearTimer(r.heartbeatTimer)
	r.leaderState = emptyLeaderState

	r.term++
	r.candidateState = CandidateState{
		isCandidate: true,
		votesGranted: map[int32]bool{r.Id: true},
	}

	r.votedFor = int(r.Id)

	args := &RequestVote{
		SenderId: r.Id, Term: int32(r.term), CommitIndex: int32(r.commitIndex), LogTerm: int32(r.logTerm), LogLength: int32(len(r.log)),
	}

	for i := 0; i < r.N; i++ {
		if i != int(r.Id) {
			r.SendMsg(int32(i), r.requestVoteRPC, args)
		}
	}

	if len(r.candidateState.votesGranted) > r.N/2 {
		r.becomeLeader()
	}
}


func (r *Replica) handleRequestVote(rpc *RequestVote) {
	r.handleIncomingTerm(rpc)

	entries := r.copySuffix(int(rpc.CommitIndex) + 1)

	if r.term > int(rpc.Term) || r.logTerm > int(rpc.GetLogTerm()) || (r.logTerm == int(rpc.GetLogTerm()) && len(r.log) > int(rpc.LogLength)) || (r.votedFor != -1 && r.votedFor != int(rpc.SenderId)) {
		args := &RequestVoteReply{
			SenderId: r.Id, Term: int32(r.term), RequestTerm: rpc.Term, CommitIndex: int32(r.commitIndex), LogTerm: int32(r.logTerm), LogLength: int32(len(r.log)),
			VoteGranted: False, StartIndex: rpc.CommitIndex + 1, Entries: entries, PQEntries: r.pq.extractList(),
		}
		r.SendMsg(rpc.SenderId, r.requestVoteReplyRPC, args)
		return
	} else {
		r.votedFor = int(rpc.SenderId)
		args := &RequestVoteReply{
			SenderId: r.Id, Term: int32(r.term), RequestTerm: rpc.Term, CommitIndex: int32(r.commitIndex), LogTerm: int32(r.logTerm), LogLength: int32(len(r.log)),
			VoteGranted: True, StartIndex: rpc.CommitIndex + 1, Entries: entries, PQEntries: r.pq.extractList(),
		}
		r.SendMsg(rpc.SenderId, r.requestVoteReplyRPC, args)

		timeout := rand.Intn(r.electionTimeout/2) + r.electionTimeout/2
		setTimer(r.electionTimer, time.Duration(timeout)*time.Millisecond)
		return
	}
}


func (r *Replica) handleRequestVoteReply (rpc *RequestVoteReply) {
	// if r.term < int(rpc.Term), then you would no longer be the candidate after r.handleIncomingTerm(rpc)
	r.handleIncomingTerm(rpc)

	if int(rpc.Term) < r.term || int(rpc.RequestTerm) != r.term {
		return
	}

	if !r.candidateState.isCandidate {
		return
	}

	if !r.updateLogIfPossible(rpc) {
		r.sendOneGetCommittedData(int(rpc.SenderId))
		return
	}

	// Do not count the grant if updating the log cancelled the campaign
	if !r.candidateState.isCandidate {
		return
	}

	if rpc.VoteGranted == True {
		r.candidateState.votesGranted[rpc.SenderId] = true
	}

	if len(r.candidateState.votesGranted) > r.N/2 {
		r.becomeLeader()
	}
}

func (r *Replica) becomeLeader() {
	fmt.Println("Replica", r.Id, "becoming the leader for term", r.term)

	r.candidateState = emptyCandidateState
	r.logTerm = r.term

	r.leaderState = LeaderState{
		isLeader: true,
		version: 1,
		repNextIndex: make([]int, r.N),
		repMatchIndex: make([]int, r.N),
	}

	r.appendQueuedEntries()
	r.leaderState.numEntries = len(r.log)

	for i := 0; i < r.N; i++ {
		if i != int(r.Id) {
			r.leaderState.repNextIndex[i] = len(r.log)
			r.leaderState.repMatchIndex[i] = 0
		}
	}

	clearTimer(r.electionTimer)
	clearTimer(r.benOrStartTimer)

	r.sendHeartbeat()
}
