package hydrofoil

import (
	"bufio"
	"bytes"
	"fastrpc"
	"fmt"
	"genericsmr"
	"genericsmrproto"
	"io"
	"math/rand"
	"reflect"
	"state"
	"testing"
	"time"
)

// The tester generously allows solutions to complete elections in one second
// (much more than the paper's range of timeouts).
const electionTimeout = 150
const heartbeatTimeout = 15
const benOrStartTimeout = 40
const benOrResendTimeout = 20

// Agreement and recovery

func TestInitialElection(t *testing.T) {
	servers := 3
	cfg := make_config_full(t, servers, false, electionTimeout, heartbeatTimeout, 1e9, 1e9)
	cfg.runReplicas()
	defer cfg.cleanup()


	fmt.Println("Test: initial election...")

	// is a leader elected?
	leader1 := cfg.checkOneLeader()
	cfg.disconnect(leader1)

	time.Sleep(4 * electionTimeout)

	cfg.checkOneLeader()
	cfg.connect(leader1)

	time.Sleep(4 * electionTimeout)

	cfg.checkOneLeader()

	fmt.Println("... Passed")
}

func TestBasicAgree(t *testing.T) {
	servers := 3
	cfg := make_config_full(t, servers, false, electionTimeout, heartbeatTimeout, 1e9, 1e9)
	cfg.runReplicas()
	defer cfg.cleanup()

	fmt.Println("Test: basic agreement...")
	cfg.checkOneLeader()

	fmt.Println("Testing agreement on 3 entries...")
	iters := 3
	for index := 1; index < iters+1; index++ {
		res := cfg.sendCommandLeaderCheckReplicas(index*100, servers)
		if !res {
			t.Fatal("Failed agreement on entry")
		}
	}

	fmt.Println(cfg.checkLogData())

	fmt.Println("... Passed")
}

func TestFailAgree(t *testing.T) {
	servers := 5
	cfg := make_config_full(t, servers, false, electionTimeout, heartbeatTimeout, 1e9, 1e9)
	cfg.runReplicas()
	defer cfg.cleanup()

	fmt.Println("Test: fail agree...")
	leader := cfg.checkOneLeader()

	cfg.disconnect((leader + 1) % 5)
	cfg.disconnect((leader + 2) % 5)

	fmt.Println("Testing agreement on 3 replicas (disconnected replica)...")
	iters := 3
	index := 1
	for ; index < iters+1; index++ {
		res := cfg.sendCommandLeader(index * 100)
		if !res {
			t.Fatal("Failed agreement on entry")
		}
	}
	fmt.Println(cfg.checkLogData())

	cfg.connect((leader + 1) % 5)
	cfg.connect((leader + 2) % 5)
	time.Sleep(4 * electionTimeout)

	leader = cfg.checkOneLeader()
	cfg.disconnect((leader + 1) % 5)
	cfg.disconnect((leader + 2) % 5)

	fmt.Println("Testing agreement on 3 replicas (disconnected leader)...")
	iters = 6
	for ; index < iters+1; index++ {
		res := cfg.sendCommandLeader(index * 100)
		if !res {
			t.Fatal("Failed agreement on entry")
		}
	}
	assert(t, len(cfg.checkLogData()) == 6, "Log length is not 6")

	cfg.disconnect((leader + 3) % 5)
	fmt.Println("Testing no agreement on replicas (disconnected leader)...")
	iters = 8
	for ; index < iters+1; index++ {
		res := cfg.sendCommandCheckCommit((leader+4)%5, index*100)
		if res {
			t.Fatal("Reached agreement even though majority of replicas are down")
		}
	}

	fmt.Println("Reconnecting everyone")
	cfg.connect((leader + 1) % 5)
	cfg.connect((leader + 2) % 5)
	cfg.connect((leader + 3) % 5)

	fmt.Println("Testing agreement on 5 replicas...")
	time.Sleep(4 * electionTimeout)
	cfg.checkOneLeader()

	fmt.Println("... Passed")
}

func TestBenOrSimple(t *testing.T) {
	servers := 3
	cfg := make_config_full(t, servers, false, 1e9, 1e9, benOrStartTimeout, benOrResendTimeout)
	cfg.runReplicas()
	defer cfg.cleanup()
	fmt.Println("Test: ben or simple...")

	res := cfg.sendCommandCheckCommit(0, 100)
	if !res {
		t.Fatal("Failed agreement on entry")
	}

	fmt.Println("... Passed")
}

func TestBenOrMultiple(t *testing.T) {
	servers := 5
	cfg := make_config_full(t, servers, false, 1e9, 1e9, benOrStartTimeout, benOrResendTimeout)
	cfg.runReplicas()
	defer cfg.cleanup()

	fmt.Println("Test: ben or multiple...")

	cfg.sendCommand(0, 100)
	cfg.sendCommand(1, 200)
	cfg.sendCommand(2, 300)
	cfg.sendCommand(3, 400)
	cfg.sendCommand(4, 500)

	time.Sleep(3 * time.Second)
	commands := cfg.checkLogData()
	fmt.Println(commandToString(commands))

	res, _ := cfg.sendCommandCheckResults(0, 600, 5)
	if !res {
		t.Fatal("Failed agreement on entry")
	}

	commands = cfg.checkLogData()
	assert(t, len(commands) == 6, "Log length is not 6")
	fmt.Println(commandToString(commands))

	fmt.Println("... Passed")
}

func TestBenOrRapidEntries(t *testing.T) {
	servers := 5
	cfg := make_config_full(t, servers, false, 1e9, 1e9, benOrStartTimeout, 10)
	cfg.runReplicas()
	defer cfg.cleanup()

	fmt.Println("Test: ben or rapid entries...")
	fmt.Println("Test 6 commands")

	oldTime := time.Now()

	cfg.sendCommand(0, 1)
	cfg.sendCommand(1, 2)
	cfg.sendCommand(2, 3)
	cfg.sendCommand(3, 4)
	cfg.sendCommand(4, 5)

	res, newTime := cfg.sendCommandCheckResults(0, 6, 5)
	if !res {
		t.Fatal("Failed agreement on entry")
	}

	fmt.Println("Time taken (5 commands): ", newTime.Sub(oldTime))
	time.Sleep(2 * time.Second)

	fmt.Println("Test 14 commands")

	oldTime = time.Now()

	cfg.sendCommand(0, 100)
	cfg.sendCommand(1, 200)
	cfg.sendCommand(2, 300)
	cfg.sendCommand(3, 400)
	cfg.sendCommand(4, 500)
	cfg.sendCommand(4, 600)
	cfg.sendCommand(3, 700)
	cfg.sendCommand(2, 800)
	cfg.sendCommand(1, 900)
	cfg.sendCommand(0, 1000)
	cfg.sendCommand(2, 1100)
	cfg.sendCommand(1, 1200)
	cfg.sendCommand(0, 1300)

	time.Sleep(500 * time.Millisecond)

	res, newTime = cfg.sendCommandCheckResults(0, 1400, 5)
	if !res {
		t.Fatal("Failed agreement on entry")
	}

	commands := cfg.checkLogData()
	assert(t, len(commands) == 20, "Log length is not 20")
	fmt.Println(commandToString(commands))

	fmt.Println("... Passed")
}

func TestFigure8(t *testing.T) {
	servers := 5
	cfg := make_config_full(t, servers, false, electionTimeout, heartbeatTimeout, 1e9, 1e9)
	cfg.runReplicas()
	defer cfg.cleanup()

	fmt.Println("Test: figure 8...")
	res := cfg.sendCommandCheckCommit(0, -1)
	if !res {
		t.Fatal("Failed agreement on entry")
	}

	nup := servers
	for iters := 0; iters < 300; iters++ {
		leader := -1
		for i := 0; i < servers; i++ {
			isLeader := cfg.sendCommand(i, iters*servers+i+1)
			if isLeader && cfg.connectedToNet[i] {
				leader = i
			}
		}

		if (rand.Int() % 1000) < 100 {
			ms := rand.Int63() % (1000 / 2)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		} else {
			ms := (rand.Int63() % 13)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}

		if leader != -1 && (rand.Int()%1000) < int(1000)/2 {
			cfg.disconnect(leader)
			nup -= 1
		}

		if nup < 3 {
			rep := rand.Int() % servers
			if cfg.connectedToNet[rep] == false {
				cfg.connect(rep)
				nup += 1
			}
		}
	}

	for i := 0; i < servers; i++ {
		if cfg.connectedToNet[i] == false {
			cfg.connect(i)
		}
	}

	time.Sleep(1 * time.Second)
	res = cfg.sendCommandLeaderCheckReplicas(10000, 5)
	if !res {
		t.Fatal("Failed agreement on entry")
	}

	commands := cfg.checkLogData()
	fmt.Println("length: ", len(commands), "data: ", commandToString(commands))
	assert(t, len(commands) == 1502, "Log length is not 1502")

	loc := make([]int, servers)
	for i := 0; i < 1500; i++ {
		opId := int(commands[i+1].OpId) % servers
		pos := (opId + servers - 1) % servers
		if opId < loc[pos] {
			t.Fatal("Out of order")
		}
		loc[pos] = opId
	}

	fmt.Println("None out of order")

	fmt.Println("... Passed")
}

func TestBenOrManyReconnects(t *testing.T) {
	servers := 5
	cfg := make_config_full(t, servers, false, 1e9, 1e9, benOrStartTimeout, 10)
	cfg.runReplicas()
	defer cfg.cleanup()

	fmt.Println("Test: ben or many reconnects...")
	res := cfg.sendCommandCheckCommit(0, -1)
	if !res {
		t.Fatal("Failed agreement on entry")
	}

	nup := servers
	for iters := 0; iters < 50; iters++ {
		for i := 0; i < servers; i++ {
			cfg.sendCommand(i, iters*servers+i)
			fmt.Println("Just sent", iters*servers+i, "to", i)
		}

		if (rand.Int() % 1000) < 100 {
			ms := rand.Int63() % (1000 / 2)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		} else {
			ms := (rand.Int63() % 13)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}

		if nup > 2 {
			rep := rand.Int() % servers
			if cfg.connectedToNet[rep] && (rand.Int()%1000) < int(1000)/2 {
				cfg.disconnect(rep)
				nup -= 1
			}
		}

		if nup < 3 {
			rep := rand.Int() % servers
			if cfg.connectedToNet[rep] == false {
				cfg.connect(rep)
				nup += 1
			}
		}
	}

	for nup < 3 {
		rep := rand.Int() % servers
		if cfg.connectedToNet[rep] == false {
			cfg.connect(rep)
			nup += 1
		}
	}

	for i := 0; i < servers; i++ {
		for j := 0; j < servers; j++ {
			fmt.Println("Connection from", i, "to", j, "is", cfg.replicas[i].IsConnected(j))
		}
	}

	time.Sleep(25 * time.Second)

	for i := 0; i < servers; i++ {
		if cfg.connectedToNet[i] == false {
			cfg.connect(i)
		}
	}

	time.Sleep(3 * time.Second)

	commands := cfg.checkLogData()
	fmt.Println(commandToString(commands))
	assert(t, len(commands) == 251, "Log length is not 251")

	loc := make([]int, servers)
	for i := 0; i < 250; i++ {
		opId := int(commands[i+1].OpId) % servers
		pos := (opId + servers - 1) % servers
		if opId < loc[pos] {
			t.Fatal("Out of order")
		}
		loc[pos] = opId
	}

	fmt.Println("None out of order")

	res, _ = cfg.sendCommandCheckResults(0, 1000, 5)
	if !res {
		t.Fatal("Failed agreement on entry")
	}

	fmt.Println("... Passed")
}

func TestRaftWithBenOrSimple(t *testing.T) {
	servers := 5
	cfg := make_config_full(t, servers, false, electionTimeout, heartbeatTimeout, benOrStartTimeout, benOrResendTimeout)
	cfg.runReplicas()
	defer cfg.cleanup()

	fmt.Println("Test: raft with ben or...")
	leader1 := cfg.checkOneLeader()

	for i := 0; i < servers; i++ {
		cfg.replicas[i].electionTimeout = 1e9
	}

	time.Sleep(1 * time.Second)

	leader2 := cfg.checkOneLeader()
	assert(t, leader1 == leader2, "Leader changed")

	for iters := 0; iters < 10; iters++ {
		for i := 0; i < servers; i++ {
			cfg.sendCommand(i, iters*servers+i+1)
			fmt.Println("Just sent", iters*servers+i+1, "to", i)
		}

		if (rand.Int() % 1000) < 100 {
			ms := rand.Int63() % (1000 / 2)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		} else {
			ms := (rand.Int63() % 13)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}
	}

	cfg.commandsExecuted()

	cfg.disconnect(leader1)

	time.Sleep(5 * time.Second)

	commands := cfg.checkLogData()
	fmt.Println("Commands of length", len(commands), ": ", commandToString(commands))

	cfg.connect(leader1)
	time.Sleep(1 * time.Second)
	commands = cfg.checkLogData()
	fmt.Println("Commands of length", len(commands), ": ", commandToString(commands))
	assert(t, len(commands) == 50, "Log length is not 50")

	loc := make([]int, servers)
	for i := 0; i < 30; i++ {
		opId := int(commands[i].OpId) % servers
		pos := (opId + servers - 1) % servers
		if opId < loc[pos] {
			t.Fatal("Out of order")
		}
		loc[pos] = opId
	}

	fmt.Println("None out of order")

	fmt.Println("... Passed")
}

func TestRaftWithBenOrNoFailures(t *testing.T) {
	servers := 5
	cfg := make_config_full(t, servers, false, electionTimeout, heartbeatTimeout, benOrStartTimeout, benOrResendTimeout)
	cfg.runReplicas()
	defer cfg.cleanup()

	fmt.Println("Test: raft with ben or no failures...")
	res := cfg.sendCommandCheckCommit(0, -1)
	if !res {
		t.Fatal("Failed agreement on entry")
	}

	for iters := 0; iters < 300; iters++ {
		for i := 0; i < servers; i++ {
			cfg.sendCommand(i, iters*servers+i+1)
		}

		if (rand.Int() % 1000) < 100 {
			ms := rand.Int63() % (1000 / 2)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		} else {
			ms := (rand.Int63() % 13)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}
	}

	time.Sleep(20 * time.Second)
	res = cfg.sendCommandLeaderCheckReplicas(10000, 5)
	if !res {
		t.Fatal("Failed agreement on entry")
	}

	commands := cfg.checkLogData()
	fmt.Println(commandToString(commands))
	assert(t, len(commands) >= 1501, "Log length is larger than 1501")

	for i := 1501; i < len(commands); i++ {
		assert(t, commands[i].OpId >= 10000, "Log entry isn't at least 10000")
	}

	loc := make([]int, servers)
	for i := 0; i < 1500; i++ {
		opId := int(commands[i+1].OpId) % servers
		pos := (opId + servers - 1) % servers
		if opId < loc[pos] {
			t.Fatal("Out of order")
		}
		loc[pos] = opId
	}

	fmt.Println("None out of order")

	fmt.Println("... Passed")
}

func TestRaftWithBenOrComplex(t *testing.T) {
	servers := 5
	cfg := make_config_full(t, servers, false, electionTimeout, heartbeatTimeout, benOrStartTimeout, benOrResendTimeout)
	cfg.runReplicas()
	defer cfg.cleanup()

	fmt.Println("Test: raft with ben or complex...")
	res := cfg.sendCommandCheckCommit(0, -1)
	if !res {
		t.Fatal("Failed agreement on entry")
	}

	nup := servers
	for iters := 0; iters < 300; iters++ {
		leader := -1
		for i := 0; i < servers; i++ {
			isLeader := cfg.sendCommand(i, iters*servers+i+1)
			if isLeader && cfg.connectedToNet[i] {
				leader = i
			}
		}

		if (rand.Int() % 1000) < 100 {
			ms := rand.Int63() % (1000 / 2)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		} else {
			ms := (rand.Int63() % 13)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}

		if leader != -1 && (rand.Int()%1000) < int(1000)/2 {
			cfg.disconnect(leader)
			nup -= 1
		}

		if nup < 3 {
			rep := rand.Int() % servers
			if cfg.connectedToNet[rep] == false {
				cfg.connect(rep)
				nup += 1
			}
		}
	}

	for i := 0; i < servers; i++ {
		if cfg.connectedToNet[i] == false {
			cfg.connect(i)
		}
	}

	time.Sleep(10 * time.Second)
	res = cfg.sendCommandLeaderCheckReplicas(10000, 5)
	if !res {
		t.Fatal("Failed agreement on entry")
	}

	commands := cfg.checkLogData()
	fmt.Println(commandToString(commands))
	assert(t, len(commands) >= 1501, "Log length is larger than 1501")

	for i := 1501; i < len(commands); i++ {
		assert(t, commands[i].OpId >= 10000, "Log entry isn't at least 10000")
	}

	loc := make([]int, servers)
	for i := 0; i < 1500; i++ {
		opId := int(commands[i+1].OpId) % servers
		pos := (opId + servers - 1) % servers
		if opId < loc[pos] {
			t.Fatal("Out of order")
		}
		loc[pos] = opId
	}

	fmt.Println("None out of order")

	fmt.Println("... Passed")
}

// Recreates the log mismatch from the writeup's Stamps section
//
// Both logs have c at index 3, but its stamps differ because R1 rewrote its log
// R2 must receive the corrected prefix before committing it, so all connected replicas execute the same sequence
func TestLogMatchingViolation(t *testing.T) {
	const (
		eTimeout  = 1000 // ms
		hbTimeout = 15
		boStart   = 40
		boResend  = 20
		huge      = 1e9
	)
	servers := 5
	cfg := make_config_full(t, servers, true, eTimeout, hbTimeout, boStart, boResend)
	cfg.runReplicas()
	defer cfg.cleanup()

	L := cfg.checkOneLeader()
	F1 := (L + 1) % servers
	G := []int{(L + 2) % servers, (L + 3) % servers, (L + 4) % servers}

	cut := func(i, j int) {
		cfg.replicas[i].SetConnected(j, false)
		cfg.replicas[j].SetConnected(i, false)
	}
	open := func(i, j int) {
		cfg.replicas[i].SetConnected(j, true)
		cfg.replicas[j].SetConnected(i, true)
	}
	// Wake the replica without changing its protocol state
	wake := func(i int) {
		cfg.replicas[i].GetStateChan <- &genericsmr.GetState{Reply: bufio.NewWriter(io.Discard)}
	}
	opIds := func(r *Replica) []int32 {
		ids := make([]int32, 0, len(r.log))
		for _, e := range r.log[1:] {
			ids = append(ids, e.Data.OpId)
		}
		return ids
	}
	execIds := func(i int) []int32 {
		ids := make([]int32, 0)
		for _, c := range cfg.repExecutions[i] {
			ids = append(ids, c.OpId)
		}
		return ids
	}
	dump := func(label string) {
		fmt.Println("---", label, "---")
		for i := 0; i < servers; i++ {
			r := cfg.replicas[i]
			fmt.Printf("replica %d: leader=%v term=%d logTerm=%d commitIndex=%d log=%v executed=%v\n",
				i, r.leaderState.isLeader, r.term, r.logTerm, r.commitIndex, opIds(r), execIds(i))
		}
	}
	ms := func(n int) time.Duration { return time.Duration(n) * time.Millisecond }

	// Delay elections and Ben-Or+ so the replicas reach the intended starting state
	cfg.replicas[F1].electionTimeout = huge
	cfg.replicas[F1].benOrStartTimeout = huge
	for _, g := range G {
		cfg.replicas[g].electionTimeout = huge
		cfg.replicas[g].benOrStartTimeout = huge
	}
	time.Sleep(ms(150)) // let the timers re-arm with the new values

	// Disconnect the leader and propose a there
	for _, g := range G {
		cut(L, g)
	}
	cut(L, F1)
	cfg.sendCommand(L, 100) // a
	time.Sleep(ms(30))

	// Propose b at the follower while it can still reach the other three replicas
	cfg.sendCommand(F1, 200) // b
	time.Sleep(ms(30))
	for _, g := range G {
		if !cfg.replicas[g].pq.contains(cfg.replicas[F1].pq.peek()) {
			t.Fatalf("SETUP: expected replica %d to hold b in its priority queue", g)
		}
	}

	// Reconnect the follower to the leader so both receive [a, b, c]
	for _, g := range G {
		cut(F1, g)
	}
	open(L, F1)
	time.Sleep(ms(80))
	cfg.sendCommand(L, 300) // c
	time.Sleep(ms(80))

	// Allow the other three replicas to commit b at index 1 using Ben-Or+
	for _, g := range G {
		cfg.replicas[g].benOrStartTimeout = boStart
		wake(g)
	}
	time.Sleep(ms(200))
	dump("after phase 4")

	want := []int32{100, 200, 300}
	if !reflect.DeepEqual(opIds(cfg.replicas[L]), want) || !reflect.DeepEqual(opIds(cfg.replicas[F1]), want) {
		t.Fatalf("SETUP: expected L and F1 logs to be %v", want)
	}
	for _, g := range G {
		r := cfg.replicas[g]
		if r.commitIndex != 1 || r.log[1].Data.OpId != 200 {
			t.Fatalf("SETUP: expected replica %d to have committed 200 at index 1 via Ben-Or+", g)
		}
	}

	// Disconnect the follower and let the leader learn the Ben-Or+ commit
	cut(L, F1)
	for _, g := range G {
		cfg.replicas[g].electionTimeout = eTimeout // used when the next heartbeat resets the timer
		open(L, g)
	}
	time.Sleep(ms(300))
	dump("after phase 5")

	restructured := []int32{200, 100, 300}
	if !reflect.DeepEqual(opIds(cfg.replicas[L]), restructured) || cfg.replicas[L].commitIndex != 3 {
		t.Fatalf("SETUP: expected L to have restructured its log to %v and committed through index 3", restructured)
	}
	for _, g := range G {
		r := cfg.replicas[g]
		if !reflect.DeepEqual(opIds(r), restructured) || r.commitIndex != 3 {
			t.Fatalf("SETUP: expected replica %d to hold %v committed through index 3", g, restructured)
		}
	}
	if !reflect.DeepEqual(opIds(cfg.replicas[F1]), want) || cfg.replicas[F1].commitIndex != 0 {
		t.Fatalf("SETUP: expected F1 to still hold %v with nothing committed", want)
	}

	// Disconnect the original leader and wait for another replica to become leader
	cfg.disconnect(L)
	time.Sleep(ms(2 * eTimeout))
	Lp := cfg.checkOneLeader()
	if Lp == L || Lp == F1 {
		t.Fatalf("SETUP: expected the new leader to come from G, got %d", Lp)
	}

	// Reconnect the stale follower to the replicas with the corrected log
	for _, g := range G {
		open(F1, g)
	}
	time.Sleep(ms(500))
	dump("after phase 7")

	// Every connected replica must execute the same sequence
	ref := execIds(G[0])
	for _, i := range append([]int{F1}, G...) {
		if !reflect.DeepEqual(execIds(i), ref) {
			t.Fatalf("SAFETY VIOLATION: replica %d executed %v but replica %d executed %v",
				i, execIds(i), G[0], ref)
		}
	}
}

// Checks that adopting a higher logTerm removes extra uncommitted entries, as described in the writeup's LogTerm section
func TestLogTermRelabelsSuffix(t *testing.T) {
	const n = 5
	const L, F1, F2, F3, F4 = 0, 1, 2, 3, 4
	names := []string{"L", "F1", "L'=F2", "F3", "F4"}
	const opA, opB, opC, opD = 100, 200, 300, 400

	// Trigger elections and heartbeats explicitly instead of waiting for timers
	net := newStepNet(t, n)

	in := func(set ...int) func(int) bool {
		return func(i int) bool {
			for _, s := range set {
				if s == i {
					return true
				}
			}
			return false
		}
	}
	among := func(set ...int) func(from, to int) bool {
		ok := in(set...)
		return func(from, to int) bool { return ok(from) && ok(to) }
	}
	propose := func(i int, op int32) {
		net.replicas[i].handleProposeCommand(state.Command{ClientId: CLIENTID, OpId: op, Op: state.PUT})
	}
	ops := func(i int) []int32 {
		r := net.replicas[i]
		ids := make([]int32, 0, len(r.log))
		for _, e := range r.log[1:] {
			ids = append(ids, e.Data.OpId)
		}
		return ids
	}
	dump := func(label string) {
		fmt.Println("---", label, "---")
		for i := 0; i < n; i++ {
			r := net.replicas[i]
			fmt.Printf("  %-6s leader=%-5v term=%d logTerm=%d commitIndex=%d log=%v queue=%v\n",
				names[i]+":", r.leaderState.isLeader, r.term, r.logTerm, r.commitIndex, ops(i), opIdsOf(r.pq.extractList()))
		}
	}
	sameOps := func(i int, want ...int32) bool { return fmt.Sprint(ops(i)) == fmt.Sprint(want) }

	// Commit a on every replica
	net.replicas[L].startElection()
	net.deliver(among(L, F1, F2, F3, F4))
	propose(L, opA)
	net.deliver(among(L, F1, F2, F3, F4))
	net.leaderTick(L)
	net.replicas[L].sendHeartbeat()
	net.deliver(among(L, F1, F2, F3, F4))
	dump("step 1: L leads term 1, a committed everywhere")
	for i := 0; i < n; i++ {
		if !sameOps(i, opA) || net.replicas[i].commitIndex != 1 {
			t.Fatalf("SETUP step 1: expected replica %s to hold [a] committed", names[i])
		}
	}

	// Replicate b and c to one follower, then stop delivering messages to or from the leader
	propose(L, opB)
	propose(L, opC)
	net.deliver(among(L, F1))
	net.leaderTick(L)
	dump("step 2: b, c only at L and F1; L crashes")
	if !sameOps(L, opA, opB, opC) || !sameOps(F1, opA, opB, opC) || net.replicas[L].commitIndex != 1 {
		t.Fatalf("SETUP step 2: expected L and F1 to hold [a b c] with only a committed")
	}

	// Elect a new leader among the three replicas that have only a
	net.replicas[F2].startElection()
	net.deliver(among(F2, F3, F4))
	dump("step 3a: F2 leads term 2 as L'")
	if !net.replicas[F2].leaderState.isLeader || !sameOps(F2, opA) {
		t.Fatalf("SETUP step 3a: expected F2 to lead term 2 with [a]")
	}

	// Deliver the new leader's heartbeat to the stale follower but hold back its reply
	net.deliver(func(from, to int) bool {
		return (from == F2 && in(F1, F3, F4)(to)) || (in(F3, F4)(from) && to == F2)
	})
	dump("step 3b: L''s heartbeat reaches F1; F1's reply is held back")

	// Commit d with the other two replicas, then disconnect this leader
	propose(F2, opD)
	net.deliver(among(F2, F3, F4))
	net.leaderTick(F2)
	dump("step 4: L' commits d at index 2")
	if net.replicas[F2].commitIndex < 2 || ops(F2)[1] != opD {
		t.Fatalf("SETUP step 4: expected L' to commit d at index 2")
	}

	// Let the previously stale follower campaign until it wins
	for attempt := 0; attempt < 3 && !net.replicas[F1].leaderState.isLeader; attempt++ {
		net.replicas[F1].startElection()
		net.deliver(among(F1, F3, F4))
		dump(fmt.Sprintf("step 5: F1's election in term %d", net.replicas[F1].term))
	}
	if !net.replicas[F1].leaderState.isLeader {
		t.Fatalf("SETUP step 5: F1 never won an election")
	}

	// Replicate and commit the elected leader's log
	for round := 0; round < 4; round++ {
		net.replicas[F1].sendHeartbeat()
		net.deliver(among(F1, F3, F4))
		net.leaderTick(F1)
	}
	dump("step 6: F1 has replicated and committed")

	// Committed entries must agree at every index
	for i := 0; i < n; i++ {
		for j := i + 1; j < n; j++ {
			ri, rj := net.replicas[i], net.replicas[j]
			upTo := min(ri.commitIndex, rj.commitIndex)
			for k := 1; k <= upTo; k++ {
				if !entryEqual(ri.log[k], rj.log[k]) {
					t.Fatalf("SAFETY VIOLATION: replica %s committed op %d at index %d but replica %s committed op %d there",
						names[i], ri.log[k].Data.OpId, k, names[j], rj.log[k].Data.OpId)
				}
			}
		}
	}
}

func TestLeaderVersionAndSnapshot(t *testing.T) {
	net := newStepNet(t, 5)
	r := net.replicas[0]
	r.term = 1
	a, b := testEntry(100, 1, -1, 0), testEntry(200, 2, -1, 0)
	r.pq.push(a)
	r.pq.push(b)
	r.becomeLeader()
	if r.leaderState.version != 1 || r.log[1].Version != 1 || r.log[2].Version != 1 {
		t.Fatal("a new leader must start with version 1")
	}

	// Keep a message containing the log before the rewrite
	request := &ReplicateEntries{Entries: r.log[1:]}
	before := append([]Entry(nil), request.Entries...)
	r.handleGetCommittedDataReply(&GetCommittedDataReply{
		SenderId: 1, Term: 1, CommitIndex: 1, LogTerm: 0,
		LogLength: 2, StartIndex: 1, Entries: []Entry{b},
	})
	if got := logOps(r); got != "[200 100]" || r.commitIndex != 1 {
		t.Fatalf("log %s, commitIndex %d; want [200 100], 1", got, r.commitIndex)
	}
	for i, entry := range request.Entries {
		if entry != before[i] {
			t.Fatalf("rewrite changed an outgoing message at entry %d", i)
		}
	}
	if r.leaderState.version != 2 {
		t.Fatalf("rewrite version %d; want 2", r.leaderState.version)
	}
	for i := 1; i < len(r.log); i++ {
		if r.log[i].Term != 1 || r.log[i].Version != 2 || r.log[i].Index != int32(i) {
			t.Fatalf("entry %d has incorrect stamp or index: %+v", i, r.log[i])
		}
	}

	// Learning an additional committed entry must extend the log without incrementing the version
	c := testEntry(300, 3, 1, 2)
	c.Index = 3
	r.handleGetCommittedDataReply(&GetCommittedDataReply{
		SenderId: 1, Term: 1, CommitIndex: 3, LogTerm: 1,
		LogLength: 4, StartIndex: 2, Entries: []Entry{r.log[2], c},
	})
	if got := logOps(r); got != "[200 100 300]" || r.leaderState.version != 2 {
		t.Fatalf("append produced log %s, version %d; want [200 100 300], 2", got, r.leaderState.version)
	}
}

func TestLogRewriteAfterTruncationPreservesSnapshot(t *testing.T) {
	net := newStepNet(t, 5)
	r := net.replicas[1]
	a, b, c := testEntry(100, 1, 1, 1), testEntry(200, 2, 1, 1), testEntry(300, 3, 1, 1)
	setLog(r, a, b, c)
	r.term, r.logTerm, r.commitIndex = 1, 1, 1
	request := &ReplicateEntries{Entries: r.log[1:]}
	before := append([]Entry(nil), request.Entries...)

	r.handleReplicateEntries(&ReplicateEntries{
		SenderId: 0, Term: 2, CommitIndex: 1, LogTerm: 2, LogLength: 2,
		PrevLogIndex: 1, Entries: []Entry{},
	})
	if got := logOps(r); got != "[100]" || r.logTerm != 2 {
		t.Fatalf("log %s, logTerm %d; want [100], 2", got, r.logTerm)
	}
	queued := r.pq.extractList()
	if len(queued) != 2 || !entryEqual(queued[0], b) || !entryEqual(queued[1], c) || !sameStamp(queued[0], b) || !sameStamp(queued[1], c) {
		t.Fatalf("displaced entries must retain their IDs and stamps in the queue: %+v", queued)
	}
	// Becoming leader gives the re-appended entries new stamps without changing the earlier message
	r.term = 3
	r.becomeLeader()
	if got := logOps(r); got != "[100 200 300]" {
		t.Fatalf("log after election %s; want [100 200 300]", got)
	}
	for _, entry := range r.log[2:] {
		if entry.Term != 3 || entry.Version != 1 {
			t.Fatalf("re-appended entry must receive the new leader's stamp: %+v", entry)
		}
	}
	for i, entry := range request.Entries {
		if entry != before[i] {
			t.Fatalf("append after truncation changed an outgoing message at entry %d", i)
		}
	}
}

// Above commitIndex, replication requires a matching non-null stamp at prevLogIndex
func TestReplicationPrefixStamp(t *testing.T) {
	net := newStepNet(t, 5)
	a, b, c := testEntry(100, 1, 1, 1), testEntry(200, 2, 1, 1), testEntry(300, 3, 1, 1)

	rpc := func(prevVersion int32) *ReplicateEntries {
		return &ReplicateEntries{SenderId: 0, Term: 1, CommitIndex: 0, LogTerm: 1, LogLength: 4,
			PrevLogIndex: 2, PrevLogServerId: b.ServerId, PrevLogTimestamp: b.Timestamp,
			PrevLogTerm: 1, PrevLogVersion: prevVersion, Entries: []Entry{c}}
	}
	for i, tc := range []struct {
		version int32
		want    string
	}{{1, "[100 200 300]"}, {2, "[100 200]"}} {
		r := net.replicas[i+1]
		r.term, r.logTerm, r.commitIndex = 1, 1, 0
		setLog(r, a, b)
		r.handleReplicateEntries(rpc(tc.version))
		if got := logOps(r); got != tc.want {
			t.Fatalf("anchor stamp (1,%d): follower log %s, want %s", tc.version, got, tc.want)
		}
	}
}

// Learning that a is committed with a null stamp under an older logTerm must leave the log [a, b] and a's stamp unchanged
func TestStampDifferenceKeepsEntries(t *testing.T) {
	net := newStepNet(t, 5)
	a, b := testEntry(100, 1, 1, 1), testEntry(200, 2, 1, 1)
	aBottom := a
	aBottom.Term, aBottom.Version = -1, 0

	r := net.replicas[2]
	r.term, r.logTerm, r.commitIndex = 1, 1, 0
	setLog(r, a, b)
	r.handleGetCommittedDataReply(&GetCommittedDataReply{SenderId: 3, Term: 1, CommitIndex: 1, LogTerm: 0,
		LogLength: 2, StartIndex: 1, Entries: []Entry{aBottom}, PQEntries: []Entry{}})

	if got := logOps(r); got != "[100 200]" || r.commitIndex != 1 {
		t.Fatalf("log %s with commit index %d; want [100 200] with commit index 1", got, r.commitIndex)
	}
	if !sameStamp(r.log[1], a) {
		t.Fatalf("entry at index 1 lost its stamp: %+v", r.log[1])
	}
}

func TestProtocolOlderHistory(t *testing.T) {
	for _, leader := range []bool{false, true} {
		t.Run(map[bool]string{false: "follower", true: "leader"}[leader], func(t *testing.T) {
			net := newStepNet(t, 3)
			r := net.replicas[0]
			r.term, r.logTerm, r.commitIndex = 7, 7, 1
			a, b, c := testEntry(1, 1, 7, 1), testEntry(2, 2, 7, 1), testEntry(3, 3, 7, 1)
			d, e := testEntry(4, 4, 5, 1), testEntry(5, 5, 5, 1)
			setLog(r, a, b, c)
			if leader {
				r.becomeLeader()
			}
			r.handleGetCommittedDataReply(&GetCommittedDataReply{SenderId: 1, Term: 7,
				CommitIndex: 2, LogTerm: 5, LogLength: 4, StartIndex: 2, Entries: []Entry{d, e}})
			want := "[1 4]"
			if leader {
				want = "[1 4 2 3 5]"
			}
			if logOps(r) != want || r.commitIndex != 2 || r.logTerm != 7 {
				t.Fatalf("log %s ci=%d logTerm=%d; want %s, 2, 7", logOps(r), r.commitIndex, r.logTerm, want)
			}
			if !leader && (!r.pq.contains(b) || !r.pq.contains(c) || !r.pq.contains(e)) {
				t.Fatal("displaced and uncommitted incoming requests must remain queued")
			}
		})
	}
}

func TestUnchangedHeartbeatKeepsLogStorage(t *testing.T) {
	net := newStepNet(t, 3)
	r := net.replicas[1]
	entries := make([]Entry, 4096)
	for i := range entries {
		entries[i] = testEntry(int32(i+1), int64(i+1), 1, 1)
	}
	setLog(r, entries...)
	r.term, r.logTerm, r.commitIndex = 1, 1, len(entries)
	storage := &r.log[0]
	r.handleReplicateEntries(&ReplicateEntries{SenderId: 0, Term: 1, LogTerm: 1, Version: 1, CommitIndex: int32(r.commitIndex), LogLength: int32(len(r.log)), PrevLogIndex: int32(r.commitIndex)})
	if &r.log[0] != storage {
		t.Fatal("an unchanged heartbeat copied the existing log")
	}
}

func TestStampUpdatePreservesMessageSnapshot(t *testing.T) {
	net := newStepNet(t, 3)
	r := net.replicas[1]
	a, b := testEntry(1, 1, 1, 1), testEntry(2, 2, 1, 1)
	setLog(r, a, b)
	r.term, r.logTerm = 2, 1
	snapshot := r.log[1:]
	a.Term, a.Version = 2, 1
	b.Term, b.Version = 2, 1
	r.handleGetCommittedDataReply(&GetCommittedDataReply{SenderId: 0, Term: 2, LogTerm: 2, LogLength: 3, StartIndex: 1, Entries: []Entry{a, b}})
	if r.log[1].Term != 2 || r.log[2].Term != 2 {
		t.Fatal("incoming stamps were not adopted")
	}
	if snapshot[0].Term != 1 || snapshot[1].Term != 1 {
		t.Fatal("stamp update changed an earlier message snapshot")
	}
}

// A delayed acknowledgement arrives after the leader changes [a, b, c] to [b, a, c], and the stale follower must still receive the corrected log
func TestDelayedReplyAfterRestructure(t *testing.T) {
	net := newStepNet(t, 5)
	A, B, C, D, E := net.replicas[0], net.replicas[1], net.replicas[2], net.replicas[3], net.replicas[4]
	vote := func(m fastrpc.Serializable) bool { _, ok := m.(*RequestVote); return ok }
	grant := func(m fastrpc.Serializable) bool { _, ok := m.(*RequestVoteReply); return ok }
	appendRPC := func(m fastrpc.Serializable) bool { _, ok := m.(*ReplicateEntries); return ok }
	ack := func(m fastrpc.Serializable) bool { _, ok := m.(*ReplicateEntriesReply); return ok }
	data := func(m fastrpc.Serializable) bool { _, ok := m.(*GetCommittedData); return ok }
	dataReply := func(m fastrpc.Serializable) bool { _, ok := m.(*GetCommittedDataReply); return ok }
	broadcast := func(m fastrpc.Serializable) bool { _, ok := m.(*BenOrBroadcast); return ok }

	A.startElection()
	for _, id := range []int{1, 2, 3, 4} {
		net.send(0, id, vote)
	}
	net.send(1, 0, grant)
	net.send(2, 0, grant)
	if !A.leaderState.isLeader {
		t.Fatal("A was not elected")
	}
	net.send(0, 1, appendRPC)
	net.send(1, 0, ack)

	// Create b, a, c in timestamp order, then deliver them to the leader in order a, b, c
	// Space out the requests so their timestamps differ across replicas
	C.handleProposeCommand(state.Command{ClientId: CLIENTID, OpId: 2, Op: state.PUT, K: 1, V: 2})
	time.Sleep(time.Millisecond)
	D.handleProposeCommand(state.Command{ClientId: CLIENTID, OpId: 1, Op: state.PUT, K: 1, V: 1})
	time.Sleep(time.Millisecond)
	E.handleProposeCommand(state.Command{ClientId: CLIENTID, OpId: 3, Op: state.PUT, K: 1, V: 3})
	for _, from := range []int{3, 2, 4} {
		net.send(from, 0, data)
	}
	if !(A.log[2].Timestamp < A.log[1].Timestamp && A.log[1].Timestamp < A.log[3].Timestamp) {
		t.Fatal("request creation timestamps were not strictly increasing")
	}
	A.sendHeartbeat()
	net.send(0, 1, appendRPC)
	oldReply := net.take(1, 0, ack) // hold back the follower's acknowledgement
	for _, to := range []int{2, 3, 4} {
		for _, from := range []int{2, 3, 4} {
			if from != to {
				net.send(from, to, data)
			}
		}
	}

	// The other three replicas run Ben-Or+ to commit b at index 1
	for _, id := range []int{2, 3, 4} {
		net.replicas[id].startBenOrPlus()
	}
	for _, to := range []int{2, 3, 4} {
		for _, from := range []int{2, 3, 4} {
			if from != to {
				net.send(from, to, broadcast)
			}
		}
	}
	for _, stage := range []uint8{StageOne, StageTwo} {
		for _, to := range []int{2, 3, 4} {
			for _, from := range []int{2, 3, 4} {
				if from == to {
					continue
				}
				net.send(from, to, func(m fastrpc.Serializable) bool {
					msg, ok := m.(*BenOrConsensus)
					return ok && msg.Stage == stage && msg.CommitIndex == 0
				})
			}
		}
	}
	for _, id := range []int{2, 3, 4} {
		r := net.replicas[id]
		if r.commitIndex != 1 || r.log[1].Data.OpId != 2 {
			t.Fatalf("replica %d did not decide b", id)
		}
	}

	A.sendOneGetCommittedData(2)
	net.send(0, 2, data)
	net.send(2, 0, dataReply)
	if A.commitIndex != 1 || len(A.log) != 4 || A.log[1].Data.OpId != 2 || A.log[2].Data.OpId != 1 || A.log[3].Data.OpId != 3 {
		t.Fatalf("unexpected rebuilt log: %v, commitIndex=%d", opIdsOf(A.log[1:]), A.commitIndex)
	}
	if A.logTerm != B.logTerm || !entryEqual(A.log[3], B.log[3]) || entryEqual(A.log[1], B.log[1]) {
		t.Fatal("setup did not produce matching endpoints with different prefixes")
	}
	net.dispatch(oldReply)

	// Retry replication until the follower receives the corrected prefix
	for attempt := 0; attempt < 3; attempt++ {
		A.sendHeartbeat()
		net.send(0, 1, func(m fastrpc.Serializable) bool {
			msg, ok := m.(*ReplicateEntries)
			return ok && msg.CommitIndex == 1
		})
		net.assertCommittedAgreement()
		net.send(1, 0, ack)
		if B.commitIndex == 1 {
			if len(B.log) != len(A.log) {
				t.Fatal("follower did not receive the complete replacement suffix")
			}
			for i := 1; i < len(A.log); i++ {
				if !entryEqual(A.log[i], B.log[i]) {
					t.Fatalf("follower differs from leader at index %d", i)
				}
			}
			if A.leaderState.repMatchIndex[1] != 3 {
				t.Fatal("current acknowledgment did not establish a match through index 3")
			}
			t.Log("delayed reply did not cause conflicting commits; follower reconciled to [b,a,c]")
			return
		}
	}
	t.Fatal("follower did not reconcile after retrying the rejected anchor")
}

func TestProtocolReplicationReplyVersions(t *testing.T) {
	net := newStepNet(t, 3)
	r := net.replicas[0]
	r.term = 1
	a, b := testEntry(1, 1, -1, 0), testEntry(2, 2, -1, 0)
	r.pq.push(a)
	r.pq.push(b)
	r.becomeLeader()
	old := &ReplicateEntriesReply{SenderId: 1, Term: 1, RequestTerm: 1, Version: 1,
		Success: True, NewRequestedIndex: 3, LogTerm: 1, LogLength: 3, StartIndex: 1,
		Entries: r.copySuffix(1)}
	r.handleGetCommittedDataReply(&GetCommittedDataReply{SenderId: 2, Term: 1,
		CommitIndex: 1, LogTerm: 0, LogLength: 2, StartIndex: 1, Entries: []Entry{b}})
	r.handleReplicateEntriesReply(old)
	if r.leaderState.repMatchIndex[1] != 0 || r.leaderState.repNextIndex[1] != 1 {
		t.Fatal("an old-version reply restored replication records after a rewrite")
	}
	fresh := *old
	fresh.Version = 2
	fresh.CommitIndex = 1
	fresh.Entries = r.copySuffix(1)
	r.handleReplicateEntriesReply(&fresh)
	if r.leaderState.repMatchIndex[1] != 2 {
		t.Fatal("current-version success was not counted")
	}
	fresh.Success, fresh.NewRequestedIndex = False, 1
	r.handleReplicateEntriesReply(&fresh)
	if r.leaderState.repMatchIndex[1] != 2 || r.leaderState.repNextIndex[1] != 3 {
		t.Fatal("a rejection must not undo an already acknowledged prefix")
	}
	old.Version, old.RequestTerm, old.NewRequestedIndex = 2, 0, 50
	r.handleReplicateEntriesReply(old)
	if r.leaderState.repMatchIndex[1] != 2 {
		t.Fatal("reply to an earlier-term request was counted")
	}
}

func TestProtocolReplyRewriteBeforeCounting(t *testing.T) {
	net := newStepNet(t, 3)
	r := net.replicas[0]
	r.term = 1
	a, b := testEntry(1, 1, -1, 0), testEntry(2, 2, -1, 0)
	r.pq.push(a)
	r.pq.push(b)
	r.becomeLeader()
	r.handleReplicateEntriesReply(&ReplicateEntriesReply{SenderId: 1, Term: 1, RequestTerm: 1,
		Version: 1, Success: True, NewRequestedIndex: 3, CommitIndex: 1, LogTerm: 0,
		LogLength: 2, StartIndex: 1, Entries: []Entry{b}})
	if r.leaderState.version != 2 || r.leaderState.repMatchIndex[1] != 0 {
		t.Fatal("must recheck version after processing a reply that rewrites the log")
	}
}

func TestProtocolElectionGrants(t *testing.T) {
	net := newStepNet(t, 5)
	r := net.replicas[0]
	r.startElection()
	grant := &RequestVoteReply{SenderId: 1, Term: 1, RequestTerm: 1, VoteGranted: True, LogLength: 1, StartIndex: 1}
	for i := 0; i < 4; i++ {
		r.handleRequestVoteReply(grant)
	}
	if r.leaderState.isLeader || len(r.candidateState.votesGranted) != 2 {
		t.Fatal("repeated grants must count only once")
	}
	r.startElection()
	grant.Term = 2 // reply to an old request, sent after the voter learned term 2
	r.handleRequestVoteReply(grant)
	if len(r.candidateState.votesGranted) != 1 {
		t.Fatal("old campaign grant was counted")
	}
	grant.RequestTerm = 2
	r.handleRequestVoteReply(grant)
	grant.SenderId = 2
	r.handleRequestVoteReply(grant)
	if !r.leaderState.isLeader {
		t.Fatal("distinct current-term grants should elect a leader")
	}
	// Repeated requests from the same candidate can receive another grant
	voter := net.replicas[3]
	request := &RequestVote{SenderId: 0, Term: 2, LogLength: 1}
	voter.handleRequestVote(request)
	voter.handleRequestVote(request)
	net.collect()
	count := 0
	for _, p := range net.pending {
		if reply, ok := p.m.(*RequestVoteReply); ok && p.from == 3 && reply.VoteGranted == True {
			count++
		}
	}
	if count != 2 {
		t.Fatal("repeat vote request from the chosen candidate should be granted")
	}
}

// A candidate whose log is replaced during its campaign must abandon the campaign and ignore its delayed grants
func TestCampaignCancelledWhenLogShrinks(t *testing.T) {
	net := newStepNet(t, 5)
	a, b, c := testEntry(100, 1, 1, 1), testEntry(200, 2, 1, 1), testEntry(300, 3, 1, 1)
	aBottom, bBottom := a, b
	aBottom.Term, aBottom.Version = -1, 0
	bBottom.Term, bBottom.Version = -1, 0

	r := net.replicas[1]
	r.term, r.logTerm, r.commitIndex = 1, 1, 0
	setLog(r, a, b, c)
	r.startElection() // request votes in term 2 with logTerm 1 and log [a, b, c]

	// Learn the committed entries [b, a] from a replica with a lower logTerm
	r.handleGetCommittedDataReply(&GetCommittedDataReply{SenderId: 4, Term: 2, CommitIndex: 2, LogTerm: 0,
		LogLength: 3, StartIndex: 1, Entries: []Entry{bBottom, aBottom}, PQEntries: []Entry{}})
	if got := logOps(r); got != "[200 100]" {
		t.Fatalf("log %s after learning the committed [b,a]; want [200 100]", got)
	}
	if r.candidateState.isCandidate {
		t.Fatal("log replacement did not immediately cancel the campaign")
	}

	grant := func(from int32, term int32, logLength int32) *RequestVoteReply {
		return &RequestVoteReply{SenderId: from, Term: term, RequestTerm: term, CommitIndex: 0, LogTerm: 1, LogLength: logLength,
			VoteGranted: True, StartIndex: 1, Entries: []Entry{}, PQEntries: []Entry{}}
	}
	for _, from := range []int32{2, 3} {
		r.handleRequestVoteReply(grant(from, 2, 4))
	}
	if r.leaderState.isLeader {
		t.Fatalf("became leader with log %s on votes granted to the log it advertised before shrinking", logOps(r))
	}
	if r.candidateState.isCandidate || r.term != 2 {
		t.Fatalf("candidate=%v term=%d; stale grants must leave the campaign cancelled", r.candidateState.isCandidate, r.term)
	}
	r.startElection()

	for _, from := range []int32{0, 4} {
		r.handleRequestVoteReply(grant(from, 3, 3))
	}
	if !r.leaderState.isLeader {
		t.Fatalf("did not win the new campaign with two votes for its current log")
	}
}

func TestProtocolInitialVoteUsesCurrentEntry(t *testing.T) {
	net := newStepNet(t, 3)
	r := net.replicas[0]
	r.term, r.logTerm = 1, 1
	a, b := testEntry(1, 1, 1, 1), testEntry(2, 2, -1, 0)
	setLog(r, a)
	r.benOrWaitTimeout = time.Hour
	r.startBenOrPlus() // broadcast a before learning that b is the majority entry
	for _, from := range []int32{1, 2} {
		r.handleBenOrBroadcast(&BenOrBroadcastReply{SenderId: from, Term: 1,
			LogLength: 1, StartIndex: 1, BenOrMsgValid: True, Iteration: 1, BroadcastEntry: b})
	}
	clearTimer(r.benOrWaitTimer)
	r.advanceBenOr()
	if !r.benOrState.haveMajEntry || !entryEqual(r.benOrState.benOrMajEntry, b) || r.benOrState.benOrVote != Vote0 {
		t.Fatal("must vote 0 when the majority entry conflicts with the current local entry")
	}
	// Stamps do not affect entry identity
	r.benOrState.benOrMajEntry = a
	r.benOrState.benOrMajEntry.Term = -1
	if r.initialBenOrVote() != Vote1 {
		t.Fatal("matching entry IDs with different stamps should allow vote 1")
	}
}

func TestProtocolCatchUpVoting(t *testing.T) {
	for _, tc := range []struct {
		name                           string
		iteration, phase               int32
		stage, label, senderVote, want uint8
	}{
		{"initial higher iteration", 2, 1, StageOne, VoteUninitialized, Vote1, Vote0},
		{"initial same iteration", 1, 1, StageOne, VoteUninitialized, Vote1, Vote0},
		{"coin higher iteration", 2, 2, StageOne, VoteQuestionMark, Vote1, Vote0},
		{"coin same iteration", 1, 2, StageOne, VoteQuestionMark, Vote1, Vote0},
		{"definite one", 1, 2, StageOne, Vote1, Vote1, Vote1},
		{"stage two", 1, 2, StageTwo, VoteQuestionMark, Vote1, Vote1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			net := newStepNet(t, 5)
			r := net.replicas[0]
			r.term, r.logTerm = 1, 1
			a, b := testEntry(1, 1, 1, 1), testEntry(2, 2, -1, 0)
			setLog(r, a)
			r.handleBenOrConsensus(&BenOrConsensus{SenderId: 1, Term: 1, LogLength: 1,
				Iteration: tc.iteration, Phase: tc.phase, Stage: tc.stage, Vote: tc.senderVote,
				PrevPhaseFinalValue: tc.label, HaveMajEntry: True, MajEntry: b})
			if r.benOrState.benOrVote != tc.want || r.benOrState.prevPhaseFinalValue != tc.label || !r.benOrState.haveMajEntry {
				t.Fatalf("vote=%d label=%d known=%v", r.benOrState.benOrVote, r.benOrState.prevPhaseFinalValue, r.benOrState.haveMajEntry)
			}
			// Requests and replies must both include prevPhaseFinalValue
			net.collect()
			seenRequest, seenReply := false, false
			for _, p := range net.pending {
				if p.from != 0 {
					continue
				}
				switch m := p.m.(type) {
				case *BenOrConsensus:
					seenRequest = true
					if m.PrevPhaseFinalValue != tc.label {
						t.Fatal("broadcast lost vote origin")
					}
				case *BenOrConsensusReply:
					seenReply = true
					if m.PrevPhaseFinalValue != tc.label {
						t.Fatal("response lost vote origin")
					}
				}
			}
			if !seenRequest || !seenReply {
				t.Fatal("catch-up must broadcast and answer the request")
			}
		})
	}
}

func TestProtocolCoinChecksLogAfterReplication(t *testing.T) {
	net := newStepNet(t, 3)
	r := net.replicas[0]
	r.term = 1
	a, b := testEntry(1, 1, 1, 1), testEntry(2, 2, -1, 0)
	r.benOrState.haveMajEntry, r.benOrState.benOrMajEntry = true, b
	r.enterBenOrConsensus(StageTwo, VoteQuestionMark)
	r.handleReplicateEntries(&ReplicateEntries{SenderId: 1, Term: 1, LogTerm: 1,
		LogLength: 2, PrevLogIndex: 0, Version: 1, Entries: []Entry{a}})
	r.handleBenOrConsensus(&BenOrConsensusReply{SenderId: 2, Term: 1, LogLength: 1,
		StartIndex: 1, BenOrMsgValid: True, Iteration: 1, Phase: 1, Stage: StageTwo,
		Vote: VoteQuestionMark, HaveMajEntry: True, MajEntry: b})
	if r.benOrState.benOrPhase != 2 || r.benOrState.benOrVote != Vote0 || r.benOrState.prevPhaseFinalValue != VoteQuestionMark {
		t.Fatal("the new local entry must bias the next coin flip to 0")
	}
}

func TestProtocolConsensusBlocksReplicationQuorums(t *testing.T) {
	for _, stage := range []uint8{StageOne, StageTwo, Broadcasting} {
		net := newStepNet(t, 3)
		leader, follower := net.replicas[0], net.replicas[1]
		leader.term = 1
		leader.pq.push(testEntry(1, 1, -1, 0))
		leader.becomeLeader()
		follower.benOrState.benOrRunning, follower.benOrState.benOrStage = true, stage
		follower.handleReplicateEntries(&ReplicateEntries{SenderId: 0, Term: 1, Version: 1,
			LogTerm: 1, LogLength: 2, Entries: leader.copySuffix(1)})
		reply := net.take(1, 0, func(m fastrpc.Serializable) bool { _, ok := m.(*ReplicateEntriesReply); return ok }).m.(*ReplicateEntriesReply)
		if (reply.Success == True) != (stage == Broadcasting) || reply.Version != 1 || reply.RequestTerm != 1 {
			t.Fatalf("unexpected replication reply in stage %d: %+v", stage, reply)
		}
		leader.benOrState.benOrRunning, leader.benOrState.benOrStage = true, stage
		leader.leaderState.repMatchIndex[1] = 1
		net.leaderTick(0)
		if (leader.commitIndex == 1) != (stage == Broadcasting) {
			t.Fatalf("leader committed in stage %d", stage)
		}
	}
}

func TestProtocolMissingPrefixAndCancelledWait(t *testing.T) {
	net := newStepNet(t, 3)
	r := net.replicas[0]
	a := testEntry(1, 1, -1, 0)
	r.pq.push(a)
	r.benOrWaitTimeout = time.Hour
	r.startBenOrPlus()
	r.handleBenOrBroadcast(&BenOrBroadcastReply{SenderId: 1, LogLength: 1, StartIndex: 1,
		BenOrMsgValid: True, Iteration: 1, BroadcastEntry: a})
	if !r.benOrWaitTimer.active {
		t.Fatal("quorum should start the optional wait")
	}
	r.handleBenOrConsensus(&BenOrConsensus{SenderId: 1, CommitIndex: 2, LogLength: 3,
		Iteration: 5, Phase: 8, Stage: StageTwo, Vote: Vote1, HaveMajEntry: True, MajEntry: a})
	if r.commitIndex != 0 || r.benOrState.benOrIteration != 1 {
		t.Fatal("message with missing committed entries changed consensus state")
	}
	net.take(0, 1, func(m fastrpc.Serializable) bool { _, ok := m.(*GetCommittedData); return ok })
	r.handleGetCommittedDataReply(&GetCommittedDataReply{SenderId: 1, CommitIndex: 1,
		LogLength: 2, StartIndex: 1, Entries: []Entry{a}})
	if r.benOrState.benOrRunning || r.benOrWaitTimer.active || r.benOrState.benOrIteration != 1 {
		t.Fatal("commit must cancel the old wait and reset the next index to iteration 1")
	}
}

func TestProtocolSingleReplica(t *testing.T) {
	net := newStepNet(t, 1)
	r := net.replicas[0]
	r.pq.push(testEntry(1, 1, -1, 0))
	r.startBenOrPlus()
	if r.commitIndex != 1 || r.benOrState.benOrRunning {
		t.Fatal("one replica should finish Ben-Or+ from its own votes")
	}
	r.startElection()
	if !r.leaderState.isLeader {
		t.Fatal("one replica's self-vote is an election quorum")
	}
}

// A follower waiting in an old Ben-Or+ iteration must leave it when the leader's reply shows a higher iteration
func TestProtocolInactiveLeaderEndsOldIteration(t *testing.T) {
	net := newStepNet(t, 3)
	leader, follower := net.replicas[0], net.replicas[1]
	leader.term = 1
	leader.pq.push(testEntry(1, 1, -1, 0))
	leader.pq.push(testEntry(2, 2, -1, 0))
	leader.becomeLeader()
	leader.benOrState.benOrIteration = 2

	follower.term, follower.logTerm = 1, 1
	setLog(follower, leader.log[1:]...)
	follower.benOrState = BenOrState{
		benOrRunning: true, benOrIteration: 1, benOrPhase: 1, benOrStage: StageTwo,
		benOrVote: Vote0, benOrConsensusMsgs: []uint8{Vote0},
		heardServerFromBroadcast: make([]bool, 3), heardServerFromConsensus: []bool{false, true, false},
		prevPhaseFinalValue: VoteUninitialized,
	}

	leader.handleBenOrConsensus(&BenOrConsensus{SenderId: 1, Term: 1, LogTerm: 1, LogLength: 3,
		Iteration: 1, Phase: 1, Stage: StageTwo, Vote: Vote0, PrevPhaseFinalValue: VoteUninitialized})
	reply := net.take(0, 1, func(m fastrpc.Serializable) bool { _, ok := m.(*BenOrBroadcastReply); return ok })
	if r := reply.m.(*BenOrBroadcastReply); r.BenOrMsgValid != False || r.Iteration != 2 {
		t.Fatalf("inactive leader should reply with its iteration and an invalid entry: %+v", r)
	}
	net.dispatch(reply)
	if !follower.benOrState.benOrRunning || follower.benOrState.benOrIteration != 2 || follower.benOrState.benOrStage != Broadcasting {
		t.Fatalf("follower stayed in the old iteration: %+v", follower.benOrState)
	}
	if len(follower.benOrState.benOrBroadcastMsgs) != 1 {
		t.Fatal("the invalid reply's entry must not count as a broadcast")
	}

	leader.sendHeartbeat()
	net.send(0, 1, func(m fastrpc.Serializable) bool { _, ok := m.(*ReplicateEntries); return ok })
	net.send(1, 0, func(m fastrpc.Serializable) bool { r, ok := m.(*ReplicateEntriesReply); return ok && r.Success == True })
	net.leaderTick(0)
	if leader.commitIndex != 2 {
		t.Fatalf("leader could not commit after the follower left the old iteration: commitIndex %d", leader.commitIndex)
	}
}

// Check that encoding and decoding preserve entry stamps and the stamp at prevLogIndex
func TestStampSerialization(t *testing.T) {
	e := testEntry(100, 7, 3, 2)
	var buf bytes.Buffer
	e.Marshal(&buf)
	var got Entry
	if err := got.Unmarshal(&buf); err != nil || got != e {
		t.Fatalf("Entry round trip: got %+v (err %v), want %+v", got, err, e)
	}

	re := &ReplicateEntries{SenderId: 1, Term: 3, CommitIndex: 4, LogTerm: 3, LogLength: 7,
		PrevLogIndex: 5, PrevLogServerId: 2, PrevLogTimestamp: 99, PrevLogTerm: 3, PrevLogVersion: 2,
		Entries: []Entry{e}}
	buf.Reset()
	re.Marshal(&buf)
	var gotRE ReplicateEntries
	if err := gotRE.Unmarshal(&buf); err != nil {
		t.Fatal(err)
	}
	if gotRE.PrevLogTerm != 3 || gotRE.PrevLogVersion != 2 || gotRE.PrevLogTimestamp != 99 ||
		len(gotRE.Entries) != 1 || gotRE.Entries[0] != e {
		t.Fatalf("ReplicateEntries round trip: got %+v", gotRE)
	}
}

func TestProtocolReplyWireFields(t *testing.T) {
	for _, message := range []fastrpc.Serializable{
		&ReplicateEntries{Version: 7},
		&ReplicateEntriesReply{RequestTerm: 8, Version: 7, Success: True},
		&RequestVoteReply{RequestTerm: 8, VoteGranted: True},
	} {
		var buf bytes.Buffer
		message.Marshal(&buf)
		switch want := message.(type) {
		case *ReplicateEntries:
			var got ReplicateEntries
			if err := got.Unmarshal(&buf); err != nil || got.Version != want.Version {
				t.Fatal("request version lost", err)
			}
		case *ReplicateEntriesReply:
			var got ReplicateEntriesReply
			if err := got.Unmarshal(&buf); err != nil || got.RequestTerm != want.RequestTerm || got.Version != want.Version || got.Success != want.Success {
				t.Fatal("replication response fields lost", err)
			}
		case *RequestVoteReply:
			var got RequestVoteReply
			if err := got.Unmarshal(&buf); err != nil || got.RequestTerm != want.RequestTerm {
				t.Fatal("election request term lost", err)
			}
		}
		if buf.Len() != 0 {
			t.Fatal("decoder left unread wire fields")
		}
	}
}

func TestSlowClientDoesNotBlockReplica(t *testing.T) {
	net := newStepNet(t, 3)
	r := net.replicas[0]
	r.State = state.InitState()
	slowReader, slowWriter := io.Pipe()
	fastReader, fastWriter := io.Pipe()
	defer slowReader.Close()
	defer slowWriter.Close()
	defer fastReader.Close()
	defer fastWriter.Close()
	r.clientWriters = map[uint32]*bufio.Writer{0: bufio.NewWriter(slowWriter), 1: bufio.NewWriter(fastWriter)}
	a, b, c := testEntry(1, 1, 1, 1), testEntry(2, 2, 1, 1), testEntry(3, 3, 1, 1)
	a.Data.K, a.Data.V = 7, 10
	b.Data.ClientId, b.Data.Op, b.Data.K = 1, state.GET, 7
	c.Data.K, c.Data.V = 7, 20
	setLog(r, a, b, c)
	r.commitIndex = 3
	finished := make(chan struct{})
	go func() { r.executeCommands(); close(finished) }()
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("command execution waited for a client to read its reply")
	}
	if r.lastApplied != 3 || r.State.Store[7] != 20 {
		t.Fatal("commands were not applied once in log order")
	}
	r.handleRequestVote(&RequestVote{SenderId: 2, Term: 2, LogLength: 4})
	if r.term != 2 {
		t.Fatal("replica did not process the next protocol message")
	}

	type response struct {
		reply genericsmrproto.ProposeReplyTS
		err   error
	}
	readReply := func(reader io.Reader) genericsmrproto.ProposeReplyTS {
		t.Helper()
		result := make(chan response, 1)
		go func() {
			var header [1]byte
			var reply genericsmrproto.ProposeReplyTS
			_, err := io.ReadFull(reader, header[:])
			if err == nil {
				err = reply.Unmarshal(reader)
			}
			result <- response{reply, err}
		}()
		select {
		case got := <-result:
			if got.err != nil {
				t.Fatal(got.err)
			}
			return got.reply
		case <-time.After(time.Second):
			t.Fatal("client did not receive its reply")
			return genericsmrproto.ProposeReplyTS{}
		}
	}
	// The fast client must receive its GET result while the slow client's replies are blocked
	if reply := readReply(fastReader); reply.CommandId != 2 || reply.Value != 10 {
		t.Fatalf("fast client received %+v; want command 2, value 10", reply)
	}
	for _, want := range []int32{1, 3} {
		if reply := readReply(slowReader); reply.CommandId != want {
			t.Fatalf("slow-client replies out of order: got %d, want %d", reply.CommandId, want)
		}
	}
}

// Test helpers

func assert(t *testing.T, cond bool, msg string) {
	if !cond {
		t.Fatal(msg)
	}
}

func testEntry(op int32, ts int64, term int32, version int32) Entry {
	return Entry{
		Data:      state.Command{ClientId: CLIENTID, OpId: op, Op: state.PUT},
		ServerId:  0,
		Timestamp: ts,
		Term:      term,
		Version:   version,
	}
}

// Replace the log and rebuild the set of entry IDs, keeping the dummy entry at index 0
func setLog(r *Replica, entries ...Entry) {
	r.log = append([]Entry{r.log[0]}, entries...)
	r.inLog = newSet()
	for _, e := range entries {
		r.inLog.add(e)
	}
}

func logOps(r *Replica) string {
	return fmt.Sprint(opIdsOf(r.log[1:]))
}

// Passes encoded messages through memory buffers and lets each test choose when to deliver them to the receiving handlers
// Replica event loops and network sockets are not used
type stepNet struct {
	t        *testing.T
	replicas []*Replica
	wires    [][]bytes.Buffer
	pending  []pendingMsg
}

type pendingMsg struct {
	from, to int
	m        fastrpc.Serializable
}

func newStepNet(t *testing.T, n int) *stepNet {
	net := &stepNet{t: t, wires: make([][]bytes.Buffer, n)}
	for i := range net.wires {
		net.wires[i] = make([]bytes.Buffer, n)
	}
	for i := 0; i < n; i++ {
		base := &genericsmr.Replica{
			Id: int32(i), N: n, Connected: make(map[int]bool),
			PeerWriters:  make([]genericsmr.LockedWriter, n),
			TestingState: genericsmr.Testing{IsProduction: true},
		}
		for j := 0; j < n; j++ {
			base.Connected[j] = true
			base.PeerWriters[j] = genericsmr.LockedWriter{Writer: bufio.NewWriter(&net.wires[i][j])}
		}
		r := &Replica{
			Replica: base, votedFor: -1, log: []Entry{emptyEntry},
			pq: newExtendedPriorityQueue(), inLog: newSet(), benOrState: emptyBenOrState,
			electionTimeout: 1000000000, heartbeatTimeout: 1000000000,
			benOrStartTimeout: 1000000000, benOrResendTimeout: 1000000000,
			heartbeatTimer: newTimer(), electionTimer: newTimer(),
			benOrStartTimer: newTimer(), benOrResendTimer: newTimer(),
			benOrWaitTimer: newTimer(), idleTimer: newTimer(), idleTimeout: 1000000000,
			replicateEntriesRPC: 1, replicateEntriesReplyRPC: 2,
			requestVoteRPC: 3, requestVoteReplyRPC: 4,
			benOrBroadcastRPC: 5, benOrBroadcastReplyRPC: 6,
			benOrConsensusRPC: 7, benOrConsensusReplyRPC: 8,
			getCommittedDataRPC: 9, getCommittedDataReplyRPC: 10,
		}
		net.replicas = append(net.replicas, r)
		t.Cleanup(func() {
			for _, timer := range []*ServerTimer{r.heartbeatTimer, r.electionTimer, r.benOrStartTimer, r.benOrResendTimer, r.benOrWaitTimer, r.idleTimer} {
				clearTimer(timer)
			}
		})
	}
	return net
}

func (net *stepNet) collect() {
	for from := range net.replicas {
		for to := range net.replicas {
			wire := &net.wires[from][to]
			for wire.Len() > 0 {
				code, err := wire.ReadByte()
				if err != nil {
					net.t.Fatal(err)
				}
				var m fastrpc.Serializable
				switch code {
				case 1:
					m = &ReplicateEntries{}
				case 2:
					m = &ReplicateEntriesReply{}
				case 3:
					m = &RequestVote{}
				case 4:
					m = &RequestVoteReply{}
				case 5:
					m = &BenOrBroadcast{}
				case 6:
					m = &BenOrBroadcastReply{}
				case 7:
					m = &BenOrConsensus{}
				case 8:
					m = &BenOrConsensusReply{}
				case 9:
					m = &GetCommittedData{}
				case 10:
					m = &GetCommittedDataReply{}
				default:
					net.t.Fatalf("unknown RPC code %d", code)
				}
				if err := m.Unmarshal(wire); err != nil {
					net.t.Fatal(err)
				}
				net.pending = append(net.pending, pendingMsg{from, to, m})
			}
		}
	}
}

func (net *stepNet) take(from, to int, match func(fastrpc.Serializable) bool) pendingMsg {
	net.t.Helper()
	net.collect()
	for i, p := range net.pending {
		if p.from == from && p.to == to && match(p.m) {
			net.pending = append(net.pending[:i], net.pending[i+1:]...)
			return p
		}
	}
	net.t.Fatalf("missing requested message %d -> %d", from, to)
	return pendingMsg{}
}

func (net *stepNet) send(from, to int, match func(fastrpc.Serializable) bool) {
	net.t.Helper()
	net.dispatch(net.take(from, to, match))
}

func (net *stepNet) deliver(allow func(from, to int) bool) {
	for {
		net.collect()
		idx := -1
		for i, p := range net.pending {
			if allow(p.from, p.to) {
				idx = i
				break
			}
		}
		if idx < 0 {
			return
		}
		p := net.pending[idx]
		net.pending = append(net.pending[:idx], net.pending[idx+1:]...)
		net.dispatch(p)
	}
}

func (net *stepNet) dispatch(p pendingMsg) {
	r := net.replicas[p.to]
	switch m := p.m.(type) {
	case *ReplicateEntries:
		r.handleReplicateEntries(m)
	case *ReplicateEntriesReply:
		r.handleReplicateEntriesReply(m)
	case *RequestVote:
		r.handleRequestVote(m)
	case *RequestVoteReply:
		r.handleRequestVoteReply(m)
	case *BenOrBroadcast:
		r.handleBenOrBroadcast(m)
	case *BenOrBroadcastReply:
		r.handleBenOrBroadcast(m)
	case *BenOrConsensus:
		r.handleBenOrConsensus(m)
	case *BenOrConsensusReply:
		r.handleBenOrConsensus(m)
	case *GetCommittedData:
		r.handleGetCommittedData(m)
	case *GetCommittedDataReply:
		r.handleGetCommittedDataReply(m)
	default:
		net.t.Fatalf("unexpected message type %T", m)
	}
}

// Run the leader's commit check and send any newly appended entries
func (net *stepNet) leaderTick(i int) {
	r := net.replicas[i]
	if !r.leaderState.isLeader {
		return
	}
	r.advanceLeaderCommit()
	if len(r.log) > r.leaderState.numEntries {
		r.leaderState.numEntries = len(r.log)
		r.sendHeartbeat()
	}
}

func (net *stepNet) assertCommittedAgreement() {
	net.t.Helper()
	for i, a := range net.replicas {
		for j := i + 1; j < len(net.replicas); j++ {
			b := net.replicas[j]
			for k := 1; k <= min(a.commitIndex, b.commitIndex); k++ {
				if !entryEqual(a.log[k], b.log[k]) {
					net.t.Fatalf("SAFETY VIOLATION: replica %d committed op %d at index %d but replica %d committed op %d there",
						i, a.log[k].Data.OpId, k, j, b.log[k].Data.OpId)
				}
			}
		}
	}
}

func opIdsOf(entries []Entry) []int32 {
	ids := make([]int32, 0, len(entries))
	for _, e := range entries {
		ids = append(ids, e.Data.OpId)
	}
	return ids
}
