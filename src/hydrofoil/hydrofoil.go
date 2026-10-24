package hydrofoil

import (
	"bufio"
	"dlog"
	"encoding/binary"
	"fastrpc"
	"fmt"
	"genericsmr"
	"genericsmrproto"
	"hydrofoilproto"
	"io"
	"log"
	"math/rand"
	"sort"
	"state"
	"sync"
	"time"
)

type Entry = hydrofoilproto.Entry
type ReplicateEntries = hydrofoilproto.ReplicateEntries
type ReplicateEntriesReply = hydrofoilproto.ReplicateEntriesReply
type RequestVote = hydrofoilproto.RequestVote
type RequestVoteReply = hydrofoilproto.RequestVoteReply
type BenOrBroadcast = hydrofoilproto.BenOrBroadcast
type BenOrBroadcastReply = hydrofoilproto.BenOrBroadcastReply
type BenOrConsensus = hydrofoilproto.BenOrConsensus
type BenOrConsensusReply = hydrofoilproto.BenOrConsensusReply
type GetCommittedData = hydrofoilproto.GetCommittedData
type GetCommittedDataReply = hydrofoilproto.GetCommittedDataReply

type RPC interface {
	GetSenderId() int32
	GetTerm() int32
	GetCommitIndex() int32
	GetLogTerm() int32
	GetLogLength() int32
}

type UpdateMsg interface {
	RPC
	GetStartIndex() int32
	GetEntries() []Entry
}

type ReplyMsg interface {
	UpdateMsg
	GetPQEntries() []Entry
}

type BenOrBroadcastMsg interface {
	RPC
	GetBenOrMsgValid() uint8
	GetIteration() int32
	GetBroadcastEntry() Entry
	GetStartIndex() int32
	GetEntries() []Entry
	GetPQEntries() []Entry
}

type BenOrConsensusMsg interface {
	RPC
	GetBenOrMsgValid() uint8
	GetIteration() int32
	GetPhase() int32
	GetStage() uint8
	GetVote() uint8
	GetPrevPhaseFinalValue() uint8
	GetHaveMajEntry() uint8
	GetMajEntry() Entry
	GetStartIndex() int32
	GetEntries() []Entry
	GetPQEntries() []Entry
}

const ( // for benOrStatus
	Broadcasting uint8 = 0
	StageOne           = 1
	StageTwo           = 2
	NotRunning         = 3
)

const (
	Vote0             uint8 = 0
	Vote1                   = 1
	VoteQuestionMark        = 2
	VoteUninitialized       = 3
)

const (
	False uint8 = hydrofoilproto.False
	True        = hydrofoilproto.True
)

func convertBoolToInteger(b bool) uint8 {
	if b {
		return True
	}
	return False
}

func convertIntegerToBool(i uint8) bool {
	if i == True {
		return true
	}
	return false
}

var zeroTime time.Time

type LeaderState struct {
	isLeader      bool
	repNextIndex  []int // next index to send to each replica
	repMatchIndex []int // index of highest known replicated entry on replica
	numEntries    int
	version       int // starts at 1 and increases when existing log entries change
}

type CandidateState struct {
	isCandidate  bool
	votesGranted map[int32]bool
}

type BenOrState struct {
	// benOrIndex = commitIndex + 1 at all times!
	benOrRunning bool

	benOrIteration int
	benOrPhase     int
	benOrStage     uint8

	benOrBroadcastEntry      Entry // entry that you initially broadcast this iteration
	benOrBroadcastMsgs       []Entry
	heardServerFromBroadcast []bool

	haveMajEntry  bool
	benOrMajEntry Entry // majority entry received in BenOrBroadcast. If none, then this is emptyEntry.

	benOrVote                uint8
	benOrConsensusMsgs       []uint8
	heardServerFromConsensus []bool

	prevPhaseFinalValue uint8 // final value from previous phase

}

type Replica struct {
	*genericsmr.Replica // extends a generic Paxos replica

	replicateEntriesChan      chan fastrpc.Serializable
	replicateEntriesReplyChan chan fastrpc.Serializable
	requestVoteChan           chan fastrpc.Serializable
	requestVoteReplyChan      chan fastrpc.Serializable
	benOrBroadcastChan        chan fastrpc.Serializable
	benOrBroadcastReplyChan   chan fastrpc.Serializable
	benOrConsensusChan        chan fastrpc.Serializable
	benOrConsensusReplyChan   chan fastrpc.Serializable
	getCommittedDataChan      chan fastrpc.Serializable
	getCommittedDataReplyChan chan fastrpc.Serializable
	replicateEntriesRPC       uint8
	replicateEntriesReplyRPC  uint8
	requestVoteRPC            uint8
	requestVoteReplyRPC       uint8
	benOrBroadcastRPC         uint8
	benOrBroadcastReplyRPC    uint8
	benOrConsensusRPC         uint8
	benOrConsensusReplyRPC    uint8
	getCommittedDataRPC       uint8
	getCommittedDataReplyRPC  uint8

	term     int
	votedFor int
	log      []Entry
	pq       ExtendedPriorityQueue // to be fixed
	inLog    Set

	commitIndex         int // index of highest log entry known to be committed
	logTerm             int // highest term seen from a leader
	lastApplied         int // index of last log entry applied to state machine
	clientReplyMu       sync.Mutex
	clientReplies       map[*bufio.Writer][]func()
	lastHeardFromLeader time.Time // time we last heard from the leader

	leaderState    LeaderState
	benOrState     BenOrState
	candidateState CandidateState

	electionTimeout    int
	heartbeatTimeout   int
	benOrStartTimeout  int
	benOrResendTimeout int
	benOrWaitTimeout   time.Duration // additional wait after collecting a quorum (zero means no wait)
	idleTimeout        time.Duration
	lastEntryTimestamp int64

	clientWriters    map[uint32]*bufio.Writer
	heartbeatTimer   *ServerTimer
	electionTimer    *ServerTimer
	benOrStartTimer  *ServerTimer
	benOrResendTimer *ServerTimer
	benOrWaitTimer   *ServerTimer
	idleTimer        *ServerTimer
}

type Instance struct {
	cmds   []state.Command
	ballot int32
}

// append a log entry to stable storage
func (r *Replica) recordInstanceMetadata(inst *Instance) {
	if !r.Durable {
		return
	}

	var b [4]byte
	binary.LittleEndian.PutUint32(b[0:4], uint32(inst.ballot))
	r.StableStore.Write(b[:])
}

// write a sequence of commands to stable storage
func (r *Replica) recordCommands(cmds []state.Command) {
	if !r.Durable {
		return
	}

	if cmds == nil {
		return
	}
	for i := 0; i < len(cmds); i++ {
		cmds[i].Marshal(io.Writer(r.StableStore))
	}
}

// sync with the stable store
func (r *Replica) sync() {
	if !r.Durable {
		return
	}

	r.StableStore.Sync()
}

// Manage Client Writers
func (r *Replica) registerClient(clientId uint32, writer *bufio.Writer) uint8 {
	fmt.Println("Registering client", clientId)
	w, exists := r.clientWriters[clientId]

	if !exists {
		r.clientWriters[clientId] = writer
		return True
	}

	if w == writer {
		return True
	}

	return False
}

var emptyEntry = Entry{
	Data:      state.Command{},
	ServerId:  -1,
	Term:      -1,
	Index:     -1,
	Timestamp: -1,
}

var emptyLeaderState = LeaderState{
	isLeader:      false,
	repNextIndex:  make([]int, 0), // replica next index
	repMatchIndex: make([]int, 0),
	numEntries:    1,
}

var emptyCandidateState = CandidateState{
	isCandidate: false,
}

var emptyBenOrState = BenOrState{
	benOrRunning: false,

	benOrIteration: 1,
	benOrPhase:     1,
	benOrStage:     NotRunning,

	benOrBroadcastEntry:      emptyEntry,
	benOrBroadcastMsgs:       make([]Entry, 0),
	heardServerFromBroadcast: make([]bool, 0),

	haveMajEntry:  false,
	benOrMajEntry: emptyEntry,

	benOrVote:                VoteUninitialized,
	benOrConsensusMsgs:       make([]uint8, 0),
	heardServerFromConsensus: make([]bool, 0),

	prevPhaseFinalValue: VoteUninitialized,
}

func newReplicaFullParam(id int, peerAddrList []string, thrifty bool, exec bool, dreply bool, durable bool,
	isProduction bool, electionTimeout int, heartbeatTimeout int, benOrStartTimeout int, benOrResendTimeout int) *Replica {
	r := &Replica{
		Replica:                   genericsmr.NewReplica(id, peerAddrList, thrifty, exec, dreply),
		replicateEntriesChan:      make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		replicateEntriesReplyChan: make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		requestVoteChan:           make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		requestVoteReplyChan:      make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		benOrBroadcastChan:        make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		benOrBroadcastReplyChan:   make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		benOrConsensusChan:        make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		benOrConsensusReplyChan:   make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		getCommittedDataChan:      make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		getCommittedDataReplyChan: make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		replicateEntriesRPC:       0,
		replicateEntriesReplyRPC:  0,
		requestVoteRPC:            0,
		requestVoteReplyRPC:       0,
		benOrBroadcastRPC:         0,
		benOrBroadcastReplyRPC:    0,
		benOrConsensusRPC:         0,
		benOrConsensusReplyRPC:    0,
		getCommittedDataRPC:       0,
		getCommittedDataReplyRPC:  0,

		term:     0,
		votedFor: -1,
		log:      []Entry{emptyEntry},
		pq:       newExtendedPriorityQueue(),
		inLog:    newSet(),

		commitIndex:         0,
		logTerm:             0,
		lastApplied:         0, // 0 entry is already applied (it's an empty entry)
		lastHeardFromLeader: time.Now(),

		leaderState:    emptyLeaderState,
		benOrState:     emptyBenOrState,
		candidateState: emptyCandidateState,

		electionTimeout:    electionTimeout,
		heartbeatTimeout:   heartbeatTimeout,
		benOrStartTimeout:  benOrStartTimeout,
		benOrResendTimeout: benOrResendTimeout,
		idleTimeout:        time.Duration(benOrStartTimeout) * time.Millisecond,

		clientWriters:    make(map[uint32]*bufio.Writer),
		heartbeatTimer:   newTimer(),
		electionTimer:    newTimer(),
		benOrStartTimer:  newTimer(),
		benOrResendTimer: newTimer(),
		benOrWaitTimer:   newTimer(),
		idleTimer:        newTimer(),
	}

	r.Durable = durable
	r.TestingState.IsProduction = isProduction
	for i := 0; i < r.N; i++ {
		r.SetConnected(i, false)
	}

	r.replicateEntriesRPC = r.RegisterRPC(new(ReplicateEntries), r.replicateEntriesChan)
	r.replicateEntriesReplyRPC = r.RegisterRPC(new(ReplicateEntriesReply), r.replicateEntriesReplyChan)
	r.requestVoteRPC = r.RegisterRPC(new(RequestVote), r.requestVoteChan)
	r.requestVoteReplyRPC = r.RegisterRPC(new(RequestVoteReply), r.requestVoteReplyChan)
	r.benOrBroadcastRPC = r.RegisterRPC(new(BenOrBroadcast), r.benOrBroadcastChan)
	r.benOrBroadcastReplyRPC = r.RegisterRPC(new(BenOrBroadcastReply), r.benOrBroadcastReplyChan)
	r.benOrConsensusRPC = r.RegisterRPC(new(BenOrConsensus), r.benOrConsensusChan)
	r.benOrConsensusReplyRPC = r.RegisterRPC(new(BenOrConsensusReply), r.benOrConsensusReplyChan)
	r.getCommittedDataRPC = r.RegisterRPC(new(GetCommittedData), r.getCommittedDataChan)
	r.getCommittedDataReplyRPC = r.RegisterRPC(new(GetCommittedDataReply), r.getCommittedDataReplyChan)

	if r.TestingState.IsProduction {
		go r.run()
	}

	return r
}

func newReplicaMoreParam(id int, peerAddrList []string, thrifty bool, exec bool, dreply bool, durable bool, isProduction bool) *Replica {
	return newReplicaFullParam(id, peerAddrList, thrifty, exec, dreply, durable, isProduction, 1000, 15, 50, 40)
}

func newReplicaMoreParamRaft(id int, peerAddrList []string, thrifty bool, exec bool, dreply bool, durable bool, isProduction bool) *Replica {
	return newReplicaFullParam(id, peerAddrList, thrifty, exec, dreply, durable, isProduction, 1000, 15, 1e9, 1e9)
}

func newReplicaMoreParamBenOr(id int, peerAddrList []string, thrifty bool, exec bool, dreply bool, durable bool, isProduction bool) *Replica {
	return newReplicaFullParam(id, peerAddrList, thrifty, exec, dreply, durable, isProduction, 1e9, 1e9, 50, 40)
}

// Entry point
func NewReplica(id int, peerAddrList []string, thrifty bool, exec bool, dreply bool, durable bool) *Replica {
	return newReplicaMoreParam(id, peerAddrList, thrifty, exec, dreply, durable, true)
}

func NewReplicaRaft(id int, peerAddrList []string, thrifty bool, exec bool, dreply bool, durable bool) *Replica {
	return newReplicaMoreParamRaft(id, peerAddrList, thrifty, exec, dreply, durable, true)
}

func NewReplicaBenOr(id int, peerAddrList []string, thrifty bool, exec bool, dreply bool, durable bool) *Replica {
	return newReplicaMoreParamBenOr(id, peerAddrList, thrifty, exec, dreply, durable, true)
}

func (r *Replica) getState() (bool, int, int, int) {
	return r.leaderState.isLeader, r.term, r.logTerm, r.commitIndex
}

// Send replies in order on each client connection without blocking the replica
func (r *Replica) sendClientReply(writer *bufio.Writer, send func()) {
	r.clientReplyMu.Lock()
	if r.clientReplies == nil {
		r.clientReplies = make(map[*bufio.Writer][]func())
	}
	r.clientReplies[writer] = append(r.clientReplies[writer], send)
	start := len(r.clientReplies[writer]) == 1
	r.clientReplyMu.Unlock()
	if !start {
		return
	}
	go func() {
		for {
			r.clientReplyMu.Lock()
			send := r.clientReplies[writer][0]
			r.clientReplyMu.Unlock()
			send()
			r.clientReplyMu.Lock()
			pending := r.clientReplies[writer]
			pending[0] = nil
			r.clientReplies[writer] = pending[1:]
			empty := len(r.clientReplies[writer]) == 0
			if empty {
				delete(r.clientReplies, writer)
			}
			r.clientReplyMu.Unlock()
			if empty {
				return
			}
		}
	}()
}

func (r *Replica) executeCommand(i int) {
	if r.TestingState.IsProduction {
		val := r.log[i].Data.Execute(r.State)
		if writer, ok := r.clientWriters[r.log[i].Data.ClientId]; ok {
			propreply := &genericsmrproto.ProposeReplyTS{
				OK:        True,
				CommandId: r.log[i].Data.OpId,
				Value:     val,
				Timestamp: r.log[i].Timestamp}
			r.sendClientReply(writer, func() { r.ReplyProposeTS(propreply, writer) })
		}
	} else {
		r.TestingState.ResponseChan <- genericsmr.RepCommand{int(r.Id), r.log[i].Data}
	}
}

func (r *Replica) runReplica() {
	go r.run()
}

func (r *Replica) executeCommands() {
	// Execution of the leader's state machine
	for r.lastApplied < r.commitIndex {
		r.lastApplied++
		r.executeCommand(r.lastApplied)
	}
}

func (r *Replica) advanceLeaderCommit() {
	if !r.leaderState.isLeader || (r.benOrState.benOrRunning && r.benOrState.benOrStage != Broadcasting) {
		return
	}

	matchIndices := append(make([]int, 0, r.N), r.leaderState.repMatchIndex...)
	sort.Sort(sort.Reverse(sort.IntSlice(matchIndices)))
	if r.commitIndex < matchIndices[r.N/2] {
		r.commitIndex = matchIndices[r.N/2]
		r.resetBenOr()
	}
}

/* Main event processing loop */
func (r *Replica) run() {
	if r.TestingState.IsProduction {
		r.ConnectToPeers()

		go r.WaitForClientConnections()
	}

	var timeout int
	timeout = rand.Intn(r.electionTimeout/2) + r.electionTimeout/2
	setTimer(r.electionTimer, time.Duration(timeout)*time.Millisecond)

	timeout = rand.Intn(r.benOrStartTimeout/2) + r.benOrStartTimeout/2
	setTimer(r.benOrStartTimer, time.Duration(timeout)*time.Millisecond)
	setTimer(r.idleTimer, r.idleTimeout)

	for !r.Shutdown {
		if r.leaderState.isLeader {
			r.advanceLeaderCommit()
			if len(r.log) > r.leaderState.numEntries {
				r.leaderState.numEntries = len(r.log)
				r.sendHeartbeat()
			}
		} else if !r.benOrState.benOrRunning && time.Since(r.lastHeardFromLeader) > time.Duration(3*r.benOrStartTimeout)*time.Millisecond &&
			(r.commitIndex+1 < len(r.log) || !r.pq.isEmpty()) {
			r.startBenOrPlus()
		}

		r.executeCommands()
		select {
		case client := <-r.RegisterClientIdChan:
			fmt.Printf("Client %d registering\n", client.ClientId)
			r.registerClient(client.ClientId, client.Reply)

		case cmd := <-r.TestingState.RequestChan:
			r.handleProposeCommand(cmd)

		case propose := <-r.ProposeChan:
			//got a Propose from a client
			dlog.Printf("Replica %d received proposal with op %d\n", r.Id, propose.Command.Op)
			r.handlePropose(propose)

		case getState := <-r.GetStateChan:
			//got a GetState request from a client
			r.handleGetState(getState)

		case slowdown := <-r.SlowdownChan:
			//got a Slowdown request from a client
			r.handleSlowdown(slowdown)

		case connect := <-r.ConnectChan:
			//got a Connect request from a client
			r.handleConnect(connect)

		case disconnect := <-r.DisconnectChan:
			//got a Disconnect request from a client
			dlog.Printf("Replica %d received Disconnect from client\n", r.Id)
			r.handleDisconnect(disconnect)

		case replicateEntriesS := <-r.replicateEntriesChan:
			replicateEntries := replicateEntriesS.(*ReplicateEntries)
			//got a ReplicateEntries message
			r.handleReplicateEntries(replicateEntries)

		case replicateEntriesReplyS := <-r.replicateEntriesReplyChan:
			replicateEntriesReply := replicateEntriesReplyS.(*ReplicateEntriesReply)
			//got a ReplicateEntriesReply message
			r.handleReplicateEntriesReply(replicateEntriesReply)

		case requestVoteS := <-r.requestVoteChan:
			requestVote := requestVoteS.(*RequestVote)
			//got a RequestVote message
			r.handleRequestVote(requestVote)

		case requestVoteReplyS := <-r.requestVoteReplyChan:
			requestVoteReply := requestVoteReplyS.(*RequestVoteReply)
			//got a RequestVoteReply message
			r.handleRequestVoteReply(requestVoteReply)

		case benOrBroadcastS := <-r.benOrBroadcastChan:
			benOrBroadcast := benOrBroadcastS.(*BenOrBroadcast)
			//got a BenOrBroadcast message
			r.handleBenOrBroadcast(benOrBroadcast)

		case benOrBroadcastReplyS := <-r.benOrBroadcastReplyChan:
			benOrBroadcastReply := benOrBroadcastReplyS.(*BenOrBroadcastReply)
			//got a BenOrBroadcastReply message
			r.handleBenOrBroadcast(benOrBroadcastReply)

		case benOrConsensusS := <-r.benOrConsensusChan:
			benOrConsensus := benOrConsensusS.(*BenOrConsensus)
			//got a BenOrConsensus message
			r.handleBenOrConsensus(benOrConsensus)

		case benOrConsensusReplyS := <-r.benOrConsensusReplyChan:
			benOrConsensusReply := benOrConsensusReplyS.(*BenOrConsensusReply)
			//got a BenOrConsensusReply message
			r.handleBenOrConsensus(benOrConsensusReply)

		case getCommittedDataS := <-r.getCommittedDataChan:
			getCommittedData := getCommittedDataS.(*GetCommittedData)
			//got a GetCommittedData message
			r.handleGetCommittedData(getCommittedData)

		case getCommittedDataReplyS := <-r.getCommittedDataReplyChan:
			getCommittedDataReply := getCommittedDataReplyS.(*GetCommittedDataReply)
			//got a GetCommittedData message
			r.handleGetCommittedDataReply(getCommittedDataReply)

		case <-r.heartbeatTimer.timer.C:
			r.heartbeatTimer.active = false
			//got a heartbeat timeout
			r.sendHeartbeat()

		case <-r.electionTimer.timer.C:
			r.electionTimer.active = false
			//got an election timeout
			r.startElection()

		case <-r.benOrStartTimer.timer.C:
			r.benOrStartTimer.active = false
			//got a benOrStartWait timeout
			r.startBenOrPlus()

		case <-r.benOrWaitTimer.timer.C:
			r.benOrWaitTimer.active = false
			r.advanceBenOr()
		case <-r.idleTimer.timer.C:
			r.idleTimer.active = false
			if !r.leaderState.isLeader && !r.benOrState.benOrRunning {
				r.sendGetCommittedData()
			} else {
				setTimer(r.idleTimer, r.idleTimeout)
			}
		case <-r.benOrResendTimer.timer.C:
			r.benOrResendTimer.active = false
			//got a benOrResend timeout
			r.resendBenOrTimer()
		}
	}

	clearTimer(r.heartbeatTimer)
	clearTimer(r.electionTimer)
	clearTimer(r.benOrStartTimer)
	clearTimer(r.benOrResendTimer)
	clearTimer(r.benOrWaitTimer)
	clearTimer(r.idleTimer)
}

func (r *Replica) sendHeartbeat() {
	timeout := rand.Intn(r.heartbeatTimeout/2) + r.heartbeatTimeout/2
	setTimer(r.heartbeatTimer, time.Duration(timeout)*time.Millisecond)

	if !r.leaderState.isLeader {
		log.Fatal("Replica", r.Id, "is not leader, but sending heartbeat")
	}

	r.broadcastReplicateEntries()
}

func (r *Replica) shutdown() {
	r.Shutdown = true
}

func (r *Replica) handleProposeCommand(cmd state.Command) {
	timestamp := time.Now().UnixNano()
	if timestamp <= r.lastEntryTimestamp {
		timestamp = r.lastEntryTimestamp + 1
	}
	r.lastEntryTimestamp = timestamp
	newLogEntry := Entry{
		Data:      cmd,
		ServerId:  r.Id,
		Term:      -1,
		Index:     -1,
		Timestamp: timestamp,
	}

	if r.leaderState.isLeader {
		newLogEntry.Term = int32(r.term)
		newLogEntry.Version = int32(r.leaderState.version)
		newLogEntry.Index = int32(len(r.log))
		r.inLog.add(newLogEntry)
		r.log = append(r.log, newLogEntry)
		r.leaderState.repNextIndex[r.Id] = len(r.log)
		r.leaderState.repMatchIndex[r.Id] = len(r.log) - 1

		r.sendHeartbeat()
	} else {
		r.pq.push(newLogEntry)
		r.sendGetCommittedData()
	}
}

func (r *Replica) handlePropose(propose *genericsmr.Propose) {
	r.handleProposeCommand(propose.Command)
}

func (r *Replica) handleGetState(getState *genericsmr.GetState) {
	args := &genericsmrproto.GetStateReply{
		IsLeader: convertBoolToInteger(r.leaderState.isLeader),
	}
	r.sendClientReply(getState.Reply, func() { r.ReplyGetState(args, getState.Reply) })
}

func (r *Replica) handleSlowdown(slowdown *genericsmr.Slowdown) {
	args := &genericsmrproto.SlowdownReply{
		Success: 1,
	}
	r.sendClientReply(slowdown.Reply, func() { r.ReplySlowdown(args, slowdown.Reply) })
	time.Sleep(time.Duration(slowdown.TimeInMs) * time.Millisecond)
}

func (r *Replica) handleConnect(connect *genericsmr.Connect) {
	for i := 0; i < r.N; i++ {
		if int32(i) != r.Id {
			r.SetConnected(i, true)
		}
	}

	args := &genericsmrproto.ConnectReply{
		Success: 1,
	}
	r.sendClientReply(connect.Reply, func() { r.ReplyConnect(args, connect.Reply) })
}

func (r *Replica) handleDisconnect(disconnect *genericsmr.Disconnect) {
	for i := 0; i < r.N; i++ {
		if int32(i) != r.Id {
			r.SetConnected(i, false)
		}
	}
	args := &genericsmrproto.DisconnectReply{
		Success: 1,
	}
	r.sendClientReply(disconnect.Reply, func() { r.ReplyDisconnect(args, disconnect.Reply) })
}

func stampValid(e Entry) bool   { return e.Term > 0 }
func sameStamp(a, b Entry) bool { return a.Term == b.Term && a.Version == b.Version }

func (r *Replica) cancelCampaign() {
	if r.candidateState.isCandidate {
		r.candidateState = emptyCandidateState
	}
}

func (r *Replica) resetBenOr() {
	r.benOrState = emptyBenOrState
	clearTimer(r.benOrResendTimer)
	clearTimer(r.benOrWaitTimer)
	setTimer(r.idleTimer, r.idleTimeout)
	if !r.leaderState.isLeader {
		timeout := rand.Intn(r.benOrStartTimeout/2) + r.benOrStartTimeout/2
		setTimer(r.benOrStartTimer, time.Duration(timeout)*time.Millisecond)
	}
}

func (r *Replica) collectEntries(entries []Entry) {
	for _, entry := range entries {
		if !r.seenBefore(entry) {
			r.pq.push(entry)
		}
	}
}

func (r *Replica) appendQueuedEntries() {
	for !r.pq.isEmpty() {
		entry := r.pq.pop()
		if r.inLog.contains(entry) {
			continue
		}
		entry.Term = int32(r.term)
		entry.Version = int32(r.leaderState.version)
		entry.Index = int32(len(r.log))
		r.log = append(r.log, entry)
		r.inLog.add(entry)
	}
	r.leaderState.repMatchIndex[r.Id] = len(r.log) - 1
	r.leaderState.repNextIndex[r.Id] = len(r.log)
}

func (r *Replica) getNewValues(msg UpdateMsg) {
	r.collectEntries(msg.GetEntries())
	if reply, ok := msg.(ReplyMsg); ok {
		r.collectEntries(reply.GetPQEntries())
	}
	if r.leaderState.isLeader {
		r.appendQueuedEntries()
	}
}

// Copy the entries so later log changes cannot alter the message
func (r *Replica) copySuffix(start int) []Entry {
	if start >= len(r.log) {
		return nil
	}
	return append([]Entry(nil), r.log[start:]...)
}

func (r *Replica) stampSuffix(first int) {
	for i := first; i < len(r.log); i++ {
		r.log[i].Term = int32(r.term)
		r.log[i].Version = int32(r.leaderState.version)
		r.log[i].Index = int32(i)
	}
}

func (r *Replica) invalidateReplicationFrom(first int) {
	for replica := 0; replica < r.N; replica++ {
		if replica != int(r.Id) {
			r.leaderState.repNextIndex[replica] = min(r.leaderState.repNextIndex[replica], first)
			r.leaderState.repMatchIndex[replica] = min(r.leaderState.repMatchIndex[replica], first-1)
		}
	}
}

func (r *Replica) hasLogData(msg UpdateMsg, start int) bool {
	first := int(msg.GetStartIndex())
	return first >= 1 && first <= max(start, r.commitIndex+1) && first+len(msg.GetEntries()) == int(msg.GetLogLength()) && int(msg.GetLogLength()) > int(msg.GetCommitIndex())
}

func (r *Replica) updateLog(msg UpdateMsg, start int) bool {
	if int(msg.GetCommitIndex()) < r.commitIndex {
		r.getNewValues(msg)
		return false
	}
	if !r.hasLogData(msg, start) {
		r.sendOneGetCommittedData(int(msg.GetSenderId()))
		return false
	}

	oldLength := len(r.log)
	firstChanged := oldLength
	changed := false
	end := int(msg.GetCommitIndex()) + 1
	if int(msg.GetLogTerm()) >= r.logTerm {
		end = int(msg.GetLogLength())
	}
	first := int(msg.GetStartIndex())
	var removed []Entry

	copied := false
	for index := max(start, r.commitIndex+1); index < end; index++ {
		entry := msg.GetEntries()[index-first]
		if index >= len(r.log) || !entryEqual(r.log[index], entry) {
			firstChanged, changed = index, true
			removed = append(removed, r.log[index:]...)
			for _, old := range r.log[index:] {
				r.inLog.remove(old)
			}
			if index < len(r.log) {
				newLog := make([]Entry, index, end)
				copy(newLog, r.log[:index])
				r.log = newLog
			}
			for j := index; j < end; j++ {
				entry = msg.GetEntries()[j-first]
				entry.Index = int32(j)
				r.log = append(r.log, entry)
				r.inLog.add(entry)
				r.pq.remove(entry)
			}
			break
		}
		if stampValid(entry) && !sameStamp(r.log[index], entry) {
			if !copied {
				r.log = append([]Entry(nil), r.log...)
				copied = true
			}
			r.log[index].Term, r.log[index].Version = entry.Term, entry.Version
		}
	}
	if int(msg.GetLogTerm()) > r.logTerm && len(r.log) > int(msg.GetLogLength()) {
		cut := int(msg.GetLogLength())
		firstChanged, changed = min(firstChanged, cut), true
		removed = append(removed, r.log[cut:]...)
		for _, old := range r.log[cut:] {
			r.inLog.remove(old)
		}
		r.log = r.log[:cut:cut]
	}
	r.logTerm = max(r.logTerm, int(msg.GetLogTerm()))
	rewritten := changed && firstChanged < oldLength
	if rewritten {
		r.cancelCampaign()
	}
	if int(msg.GetCommitIndex()) > r.commitIndex {
		r.commitIndex = int(msg.GetCommitIndex())
		r.resetBenOr()
	}
	if r.leaderState.isLeader && rewritten {
		r.leaderState.version++
		r.invalidateReplicationFrom(firstChanged)
	}
	r.collectEntries(removed)
	r.getNewValues(msg)
	if r.leaderState.isLeader && changed {
		r.stampSuffix(firstChanged)
	}
	return changed
}

func (r *Replica) updateLogFromRPC(msg ReplyMsg) bool {
	return r.updateLog(msg, r.commitIndex+1)
}

func (r *Replica) sendOneGetCommittedData(destId int) {
	entries := make([]Entry, 0)
	if r.commitIndex+1 < len(r.log) {
		entries = r.log[r.commitIndex+1:]
	}

	args := &GetCommittedData{
		SenderId: r.Id, Term: int32(r.term), CommitIndex: int32(r.commitIndex), LogTerm: int32(r.logTerm), LogLength: int32(len(r.log)),
		StartIndex: int32(r.commitIndex) + 1, Entries: entries, PQEntries: r.pq.extractList(),
	}
	r.SendMsg(int32(destId), r.getCommittedDataRPC, args)
}

func (r *Replica) sendGetCommittedData() {
	setTimer(r.idleTimer, r.idleTimeout)

	entries := make([]Entry, 0)
	if r.commitIndex+1 < len(r.log) {
		entries = r.log[r.commitIndex+1:]
	}

	args := &GetCommittedData{
		SenderId: r.Id, Term: int32(r.term), CommitIndex: int32(r.commitIndex), LogTerm: int32(r.logTerm), LogLength: int32(len(r.log)),
		StartIndex: int32(r.commitIndex) + 1, Entries: entries, PQEntries: r.pq.extractList(),
	}
	for i := 0; i < r.N; i++ {
		if int32(i) != r.Id {
			r.SendMsg(int32(i), r.getCommittedDataRPC, args)
		}
	}
}

func (r *Replica) handleGetCommittedData(rpc *GetCommittedData) {
	r.handleIncomingTerm(rpc)

	if !r.updateLogIfPossible(rpc) {
		r.sendOneGetCommittedData(int(rpc.GetSenderId()))
		return
	}

	entries := r.copySuffix(int(rpc.CommitIndex) + 1)

	args := &GetCommittedDataReply{
		SenderId: r.Id, Term: int32(r.term), CommitIndex: int32(r.commitIndex), LogTerm: int32(r.logTerm), LogLength: int32(len(r.log)),
		StartIndex: rpc.CommitIndex + 1, Entries: entries, PQEntries: r.pq.extractList(),
	}
	r.SendMsg(rpc.SenderId, r.getCommittedDataReplyRPC, args)
}

func (r *Replica) handleGetCommittedDataReply(rpc *GetCommittedDataReply) {
	r.handleIncomingTerm(rpc)

	if r.isLogMoreUpToDate(rpc) == LessUpToDate {
		r.updateLogFromRPC(rpc)
	} else {
		r.getNewValues(rpc)
	}
}
