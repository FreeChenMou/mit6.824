package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"math/rand"

	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//

const electionTimeout = 1 * time.Second

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Entry struct {
	Term    int
	Command interface{}
}

type Log struct {
	Log   []Entry
	Index int
}

type Tstate int

const (
	Leader Tstate = iota
	Follower
	Candidate
)

type InstallSnapshotArgs struct {
	// Your data here (2A, 2B).
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	applyCh   chan ApplyMsg
	applyCond *sync.Cond
	// Your data here (2A, 2B, 2C).
	state        Tstate
	electionTime time.Time

	//Persistent state
	currentTerm int
	votedFor    int
	log         Log

	//Volatile state
	commitIndex int
	lastApplied int

	//Leader state
	nextIndex  []int
	matchIndex []int
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	snapshotLastIndex int
	snapshotLastTerm  int
}

func min(num1 int, num2 int) int {
	if num1 > num2 {
		return num2
	}
	return num1
}

func max(num1 int, num2 int) int {
	if num1 < num2 {
		return num2
	}
	return num1
}

func makeLogEmpty() Log {
	return Log{make([]Entry, 1), 0}
}

func makeLog(log []Entry, index int) Log {
	return Log{log, index}
}

func (l *Log) append(entry Entry) {
	l.Log = append(l.Log, entry)
}

func (l *Log) start() int {
	return l.Index
}

func (l *Log) cutend(index int) {
	l.Log = l.Log[:index-l.Index]
}

func (l *Log) cutstart(index int) {
	l.Index += index
	l.Log = l.Log[index:]
}

func (l *Log) lastIndex() int {
	return l.Index + len(l.Log) - 1
}

func (l *Log) lastEntry() *Entry {
	return l.entry(l.lastIndex())
}

func (l *Log) entry(i int) *Entry {
	return &(l.Log[i-l.Index])
}

func (l *Log) slice(next int) []Entry {
	return l.Log[next-l.Index:]
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isleader := rf.state == Leader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	rf.persister.SaveRaftState(rf.encodeState())
}

func (rf *Raft) encodeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.snapshotLastIndex)
	e.Encode(rf.snapshotLastTerm)
	return w.Bytes()
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	var currentTerm, votedFor, lastIncludedIndex, lastIncludedTerm int
	log := makeLogEmpty()
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		PrettyDebug(dTimer, "{Node %v} restores persisted state failed", rf.me)
	}
	rf.currentTerm, rf.votedFor, rf.log, rf.snapshotLastIndex, rf.snapshotLastTerm = currentTerm, votedFor, log, lastIncludedIndex, lastIncludedTerm
	// there will always be at least one entry in rf.logs
	rf.lastApplied, rf.commitIndex = rf.log.Index, rf.log.Index
}

func (rf *Raft) persistSnapshot(snapshot []byte) {
	rf.persister.SaveStateAndSnapshot(rf.encodeState(), snapshot)
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// fmt.Printf("Cond index: %v Term: %v current commit: %v lastIndex: %v log: %v\n", lastIncludedIndex, lastIncludedTerm,
	// 	rf.commitIndex, rf.log.lastIndex(), rf.log)
	if lastIncludedIndex <= rf.commitIndex {
		return false
	}

	if lastIncludedIndex >= rf.log.lastIndex() {
		rf.log = makeLogEmpty()
	} else {
		index := lastIncludedIndex - rf.log.start()
		rf.log.cutstart(index)
		rf.log.entry(rf.log.start()).Command = nil
	}
	rf.log.Index = lastIncludedIndex
	rf.log.entry(rf.log.start()).Term = lastIncludedTerm
	rf.lastApplied, rf.commitIndex = lastIncludedIndex, lastIncludedIndex
	rf.snapshotLastIndex, rf.snapshotLastTerm = lastIncludedIndex, lastIncludedTerm
	rf.persistSnapshot(snapshot)
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//fmt.Printf("id:%v pre cut log:%v \n", rf.me, rf.log)
	start := rf.log.start()
	if index <= start {
		return
	}
	rf.snapshotLastIndex, rf.snapshotLastTerm = index, rf.log.entry(index).Term
	rf.log.cutstart(index - start)
	rf.log.entry(index).Command = nil
	//fmt.Printf("id:%v Snapshot index: %v current commit: %v lastIndex: %v log:%v \n", rf.me, index, rf.commitIndex, rf.log.lastIndex(), rf.log)
	rf.persistSnapshot(snapshot)
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()

	// fmt.Printf("id:%v InstallSnapshot index: %v Term: %v current commit: %v lastIndex: %v \n", rf.me, args.LastIncludedIndex,
	// 	args.LastIncludedTerm, rf.commitIndex, rf.log.lastIndex())
	reply.Term = rf.currentTerm

	if args.Term > rf.currentTerm {
		rf.newTerm(args.Term)
	}
	rf.setElectionTime()
	var msg ApplyMsg
	if rf.currentTerm == args.Term {
		if rf.commitIndex < args.LastIncludedIndex {
			msg = ApplyMsg{
				SnapshotValid: true,
				Snapshot:      args.Data,
				SnapshotIndex: args.LastIncludedIndex,
				SnapshotTerm:  args.LastIncludedTerm,
			}
		}
	}
	rf.mu.Unlock()
	if msg.SnapshotValid {
		rf.applyCh <- msg
	}
}

func (rf *Raft) sendSanpshot(peer int) {
	args := &InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LastIncludedIndex: rf.snapshotLastIndex,
		LastIncludedTerm:  rf.snapshotLastTerm,
		Data:              rf.persister.ReadSnapshot(),
	}
	go func() {
		var reply InstallSnapshotReply
		ok := rf.sendInstallSnapshot(peer, args, &reply)
		if ok {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			rf.processSnapshot(peer, args, &reply)
		}
	}()
}

func (rf *Raft) processSnapshot(peer int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	if reply.Term > rf.currentTerm {
		rf.newTerm(reply.Term)
	} else if reply.Term == rf.currentTerm {
		//fmt.Printf("id:%v index:%v \n", peer, args.LastIncludedIndex)
		rf.nextIndex[peer] = args.LastIncludedIndex + 1
		rf.matchIndex[peer] = args.LastIncludedIndex
	}
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entry        []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	Conflict      bool
	ConflictFirst int
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	defer rf.mu.Unlock()
	rf.mu.Lock()

	if args.Term > rf.currentTerm {
		rf.newTerm(args.Term)
	}
	index := rf.log.lastIndex()
	term := rf.log.entry(index).Term
	flag := (args.LastLogTerm == term && index <= args.LastLogIndex) || args.LastLogTerm > term
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
	} else if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && flag {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.persist()
		rf.setElectionTime()
	} else {
		reply.VoteGranted = false
	}
	reply.Term = rf.currentTerm
}

func (rf *Raft) requestVote(target int, args *RequestVoteArgs, votes *int) {
	var reply RequestVoteReply
	ok := rf.sendRequestVote(target, args, &reply)

	if ok {
		defer rf.mu.Unlock()
		rf.mu.Lock()

		//reply term more than current term -> convert to follower
		if rf.currentTerm < reply.Term {
			rf.newTerm(reply.Term)
		}
		if reply.VoteGranted {
			*votes += 1
			if *votes > len(rf.peers)/2 {
				if rf.currentTerm == args.Term {
					rf.becomeLeader()
					rf.sendAppends(true)
				}
			}
		}

	}
}

func (rf *Raft) requestVotesL() {
	args := &RequestVoteArgs{rf.currentTerm, rf.me,
		rf.log.lastIndex(), rf.log.entry(rf.log.lastIndex()).Term}
	votes := 1
	for i, _ := range rf.peers {
		if i != rf.me {
			go rf.requestVote(i, args, &votes)
		}
	}
}

func (rf *Raft) newTerm(term int) {
	rf.state = Follower
	rf.votedFor = -1
	rf.currentTerm = term
	rf.persist()
}

func (rf *Raft) becomeLeader() {
	rf.state = Leader
	PrettyDebug(dTimer, "leaderID is %v Term is %v \n", rf.me, rf.currentTerm)
	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.log.lastIndex() + 1
	}
}

func (rf *Raft) sendAppends(heartbeat bool) {
	for i, _ := range rf.peers {
		if i != rf.me {
			if rf.snapshotLastIndex > (rf.nextIndex[i] - 1) {
				rf.sendSanpshot(i)
			} else {
				rf.sendAppend(i, heartbeat)
			}

		}
	}
}

func (rf *Raft) sendAppend(peer int, heartbeat bool) {
	next := rf.nextIndex[peer]
	if next <= rf.log.start() {
		next = rf.log.start() + 1
	}
	if next-1 > rf.log.lastIndex() {
		next = rf.log.lastIndex()
	}
	args := &AppendEntriesArgs{rf.currentTerm, rf.me, next - 1,
		rf.log.entry(next - 1).Term, make([]Entry, rf.log.lastIndex()-next+1),
		rf.commitIndex}
	copy(args.Entry, rf.log.slice(next))
	go func() {
		var reply AppendEntriesReply
		ok := rf.sendAppendEntries(peer, args, &reply)
		if ok {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			rf.processAppendReply(peer, args, &reply)
		}
	}()
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//if current_Term > args.Term -> leader should be change

	reply.Term = rf.currentTerm
	reply.Success = false

	if rf.currentTerm < args.Term {
		rf.newTerm(args.Term)
	}

	if rf.currentTerm > args.Term {
		return
	}

	rf.setElectionTime()

	if args.PrevLogIndex < rf.snapshotLastIndex {
		return
	}

	if args.PrevLogIndex > rf.log.lastIndex() || rf.log.entry(args.PrevLogIndex).Term != args.PrevLogTerm ||
		(args.PrevLogIndex == rf.snapshotLastIndex && args.PrevLogTerm != rf.snapshotLastTerm) {
		var index int
		if args.PrevLogIndex <= rf.log.lastIndex() {
			confictTerm := rf.log.entry(args.PrevLogIndex).Term
			index = args.PrevLogIndex
			for rf.log.entry(index).Term == confictTerm {
				index--
			}
		} else {
			index = rf.log.lastIndex() + 1
		}
		reply.Conflict = true
		reply.ConflictFirst = index
		return
	}
	if len(args.Entry) > 0 {
		log := make([]Entry, 0)
		for i := rf.log.start(); i <= args.PrevLogIndex; i++ {
			log = append(log, *rf.log.entry(i))
		}
		log = append(log, args.Entry...)
		newLog := makeLog(log, rf.log.Index)

		if update(newLog.lastIndex(), newLog.lastEntry().Term, rf.log.lastIndex(), rf.log.lastEntry().Term) {
			rf.log = newLog
		}
		rf.persist()
		//fmt.Printf("id: %v log: %v commit:%d Entry: %v term: %v preindex: %v preTerm %v \n", rf.me, rf.log, rf.commitIndex, args.Entry, args.Term, args.PrevLogIndex, args.PrevLogTerm)
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.log.lastIndex())
		rf.signalApplier()
		//fmt.Printf("id: %v log: %v commit:%d Entry: %v lastappid:%v \n", rf.me, rf.log, rf.commitIndex, args.Entry, rf.lastApplied)
	}

	reply.Success = true

}

func update(lastLogIndex1 int, lastLogTerm1 int, lastLogIndex2 int, lastLogTerm2 int) bool {
	res := false
	if lastLogTerm1 != lastLogTerm2 {
		res = lastLogTerm1 > lastLogTerm2
	} else {
		res = lastLogIndex1 > lastLogIndex2
	}
	return res
}

func (rf *Raft) processAppendReplyTerm(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if reply.Success {
		next := args.PrevLogIndex + len(args.Entry) + 1
		match := args.PrevLogIndex + len(args.Entry)
		if next > rf.nextIndex[peer] {
			rf.nextIndex[peer] = next
		}
		if match > rf.matchIndex[peer] {
			rf.matchIndex[peer] = match
		}
		rf.commitLog()
	} else if reply.Conflict {
		rf.nextIndex[peer] = reply.ConflictFirst
	}

}

func (rf *Raft) processAppendReply(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if reply.Term > rf.currentTerm {
		rf.newTerm(reply.Term)
	} else if reply.Term == rf.currentTerm {
		rf.processAppendReplyTerm(peer, args, reply)
	}
}

func (rf *Raft) commitLog() {
	if rf.state != Leader {
		//fmt.Printf("commitLog : state is %v \n", rf.state)
	}
	start := max(rf.commitIndex+1, rf.log.start())
	for i := start; i <= rf.log.lastIndex(); i++ {
		// Leader has commit can't delete or cover previous
		if rf.log.entry(i).Term != rf.currentTerm {
			continue
		}
		n := 1
		// count pass by compare with other server match index
		for j := 0; j < len(rf.peers); j++ {
			if j != rf.me && rf.matchIndex[j] >= i {
				n += 1
			}
		}

		// more than 1/2 -> able to commit
		if n > len(rf.peers)/2 {
			rf.commitIndex = i

		}
	}
	rf.signalApplier()
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	//fmt.Printf("%d %d %v %v \n", rf.me, server, args, reply)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	//fmt.Printf("leaderID %v: send %v entry %v %v\n", args.LeaderId, peer, args, reply)
	ok := rf.peers[peer].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(peer int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	//fmt.Printf("leaderID %v: send %v entry %v %v\n", args.LeaderId, peer, args, reply)
	ok := rf.peers[peer].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2B).
	if rf.state != Leader {
		return -1, rf.currentTerm, false
	}
	e := Entry{rf.currentTerm, command}
	index := rf.log.lastIndex() + 1
	rf.log.append(e)
	rf.persist()
	//fmt.Printf("current id: %v index: %v entry: %v \n", rf.me, index, e)
	rf.sendAppends(false)

	return index, rf.currentTerm, true
}

func (rf *Raft) startElection() {
	//start election
	rf.currentTerm += 1
	rf.state = Candidate
	rf.votedFor = rf.me
	PrettyDebug(dTimer, "competition is %v Term is %v \n", rf.me, rf.currentTerm)
	rf.persist()
	rf.requestVotesL()
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.tick()
		ms := 50
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) tick() {
	defer rf.mu.Unlock()
	rf.mu.Lock()
	if rf.state == Leader {
		rf.setElectionTime()
		rf.sendAppends(true)
	}
	if time.Now().After(rf.electionTime) {
		rf.setElectionTime()
		rf.startElection()
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	rf.state = Follower
	rf.setElectionTime()

	rf.votedFor = -1
	rf.log = makeLogEmpty()

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applier()
	return rf
}

func (rf *Raft) setElectionTime() {
	t := time.Now()
	t = t.Add(electionTimeout)
	ms := rand.Int63() % 300
	t = t.Add(time.Duration(ms) * time.Millisecond)
	rf.electionTime = t
}

func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for !rf.killed() {
		last := rf.lastApplied + 1
		if last <= rf.commitIndex && last <= rf.log.lastIndex() {
			rf.lastApplied += 1
			am := ApplyMsg{
				CommandValid: true,
				CommandIndex: last,
				Command:      rf.log.entry(last).Command,
			}
			rf.mu.Unlock()
			rf.applyCh <- am
			rf.mu.Lock()
		} else {
			rf.applyCond.Wait()
		}
	}
}

func (rf *Raft) signalApplier() {
	rf.applyCond.Broadcast()
}
