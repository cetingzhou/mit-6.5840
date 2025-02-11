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
	//	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

const (
	Leader    = "leader"
	Follower  = "follower"
	Candidate = "candidate"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
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

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int64               // this peer's index into peers[]
	dead      int64               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	PersistentState

	// Volatile state on all servers
	// index of highest log entry known to be committed (initialized to 0, increases monotonically
	commitIndex atomic.Int64
	// index of highest log entry applied to state machine (initialized to 0, increases monotonically)
	lastApplied atomic.Int64

	// Volatile state on leaders
	// for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	nextIndex []int64
	// for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
	matchIndex []int64

	state atomic.Value

	electionTimeout time.Duration
	lastHeard       time.Time

	applyCh chan ApplyMsg
}

type PersistentState struct {
	// latest term server has seen (initialized to 0 on first boot,  increases monotonically)
	CurrentTerm atomic.Int64
	// candidateId that received vote in current term (or null if none)
	VotedFor atomic.Int64
	// log entries; each entry contains command for state machine, and term when entry was received by leader
	LogEntries []Entry
}

type Entry struct {
	Command interface{} // command for state machine
	Term    int64       // term when entry was received by leader
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	return int(rf.CurrentTerm.Load()), rf.state.Load() == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int64 // candidate's term
	CandidateId  int64 // candidate requesting vote
	LastLogIndex int64 // index of candidate’s last log entry
	LastLogTerm  int64 // term of candidate’s last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int64 // currentTerm, for candidate to update itself
	VoteGranted bool  // true means candidate received vote
}

// RequestVote RPC handler. Server can only vote for one candiate per term.
// A vote will be denied when any one of the following condition happens:
//  1. Candidate's term is smaller than server's current term;
//  2. Candidate's term is equal to server's current term but the server didn't vote for the candidate;
//  3. Candidate's last entry in the log is not at least up-to-date:
//     a. If the logs have last entries with different terms, then the log with the later term is more up-to-date
//     b. If the logs end with the same term, then whichever log is longer is more up-to-date
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	lastIndex, lastTerm := rf.getPrevLogIndexAndTerm()
	if args.Term < rf.CurrentTerm.Load() ||
		(args.Term == rf.CurrentTerm.Load() && args.CandidateId != rf.VotedFor.Load()) ||
		args.LastLogTerm < lastTerm ||
		(args.LastLogTerm == lastTerm && args.LastLogIndex < lastIndex) {
		reply.VoteGranted = false
	} else {
		reply.VoteGranted = true
		rf.VotedFor.Store(args.CandidateId)
		rf.CurrentTerm.Store(args.Term)
		// when server A doesn't receive heartbeats from leader B due to a network issue of A, A starts an election
		// and network issue recover. A could receive a majority of votes and becomes leader. In this case, B should
		// convert to follower since A has higher term.
		rf.state.Store(Follower)
	}
	reply.Term = rf.CurrentTerm.Load()
}

type AppendEnriesArgs struct {
	Term              int64   // leader's term
	LeaderId          int64   // so follower can redirect clients
	PrevLogIndex      int64   // index of log entry immediately preceding new ones
	PrevLogTerm       int64   // term of prevLogIndex entry
	Entries           []Entry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommitIndex int64   // leader's commitIndex
}

type AppendEntriesReply struct {
	Term    int64 // currentTerm, for leader to update itself
	Success bool  // true if follower contained entry matching prevLogIndex and prevLogTerm
}

// when a leader A can't connect to other peers due to network issue and network recovered,
// but the system has elected a new leader B when A was unavailble.
func (rf *Raft) AppendEntries(args *AppendEnriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	fmt.Printf("Peer %d received AppendEntries: %v\n", rf.me, args)
	lastIndex, _ := rf.getPrevLogIndexAndTerm()
	if args.Term < rf.CurrentTerm.Load() {
		fmt.Printf("Peer %d has higher term than current leader %d: %d > %d\n", rf.me, args.LeaderId, rf.CurrentTerm.Load(), args.Term)
		reply.Success = false
	} else if lastIndex >= 0 && args.PrevLogIndex > lastIndex {
		fmt.Printf("Peer %d is inconsistent with leader %d: peer.lastIndex is %d, but leader.PrevLogIndex is %d\n", rf.me, args.LeaderId, lastIndex, args.PrevLogIndex)
		reply.Success = false
	} else {
		if args.Entries != nil {
			if args.PrevLogIndex >= 0 && args.PrevLogTerm != rf.LogEntries[args.PrevLogIndex].Term {
				fmt.Printf("Peer %d has conflict with leader %d: peer.LogEntries[%d].Term %d != leader.LogEntries[%d].Term %d\n", rf.me, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.PrevLogIndex, rf.LogEntries[args.PrevLogIndex].Term)
				rf.LogEntries = rf.LogEntries[:args.PrevLogIndex]
				reply.Success = false
			} else {
				fmt.Printf("Peer %d appended entries from leader %d: %v\n", rf.me, args.LeaderId, args.Entries)
				rf.LogEntries = append(rf.LogEntries, args.Entries...)
				reply.Success = true
			}
		} else {
			reply.Success = true
		}
	}
	// all successful AppendEntries should commit entries if there is any log that is not committed
	if reply.Success {
		for i := rf.commitIndex.Load() + 1; i <= args.LeaderCommitIndex; i++ {
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.LogEntries[i].Command,
				CommandIndex: int(i) + 1, // test start with 1 but we start with 0
			}
			rf.commitIndex.Add(1)
			rf.lastApplied.Add(1)
			fmt.Printf("Peer %d is committing msg: %v, peer.commitIndex: %d, peer.lastApplied: %d\n", rf.me, msg, rf.commitIndex.Load(), rf.lastApplied.Load())
			rf.applyCh <- msg
			fmt.Printf("Peer %d committed msg: %v, peer.commitIndex: %d, peer.lastApplied: %d\n", rf.me, msg, rf.commitIndex.Load(), rf.lastApplied.Load())
		}
		rf.CurrentTerm.Store(args.Term)
		rf.state.Store(Follower)
	}
	rf.resetElectionTimeout()
	rf.lastHeard = time.Now()
	reply.Term = rf.CurrentTerm.Load()
	fmt.Printf("Peer %d finished AppendEntries: %v\n", rf.me, args)
}

// resetElectionTimeout resets election timeout
// Because the tester limits you tens of heartbeats per second, you will have to use an election
// timeout larger than the paper's 150 to 300 milliseconds, but not too large, because then you
// may fail to elect a leader within five seconds.
func (rf *Raft) resetElectionTimeout() {
	ms := 500 + (rand.Int63() % 500)
	rf.electionTimeout = time.Duration(ms) * time.Millisecond
}

func (rf *Raft) shouldStartElection() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state.Load() != Leader && time.Since(rf.lastHeard) > rf.electionTimeout
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
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
	index := len(rf.LogEntries) + 1 // the test start the index from 1
	term := rf.CurrentTerm.Load()
	isLeader := rf.state.Load() == Leader
	fmt.Printf("Start: {me: %d, index: %d, term: %d, isLeader: %v}\n", rf.me, index, term, isLeader)
	rf.mu.Unlock()

	if !isLeader {
		time.Sleep(time.Duration(200) * time.Millisecond)
		return int(index), int(term), isLeader
	}

	// Your code here (2B).

	// this function needs to create an async go routine to do these steps:
	// 1. write command into logs;
	// 2. send AppendEntries to all followers asynchronously in a goroutine for each follower;
	// 3. an infinite loop to sync log for each follower via AppendEntries RPC until succeed or found higher term or killed;
	// 4. once receive majority of success, commit the entry immediately by sending ApplyMsg to applyCh
	go func() {
		var countReplica atomic.Int64
		replicateSucceed := make(chan bool)
		rf.mu.Lock()
		entry := Entry{command, rf.CurrentTerm.Load()}
		rf.LogEntries = append(rf.LogEntries, entry)
		countReplica.Add(1)
		rf.mu.Unlock()
		for i := range rf.peers {
			if i != int(rf.me) {
				go func(peerIndex int) {
					fmt.Printf("Appending: {leader: %d, peer: %d, entry: %v}\n", rf.me, peerIndex, entry)
					for !rf.killed() {
						// leader needs to send all entries from the receiver's nextIndex to this new entry
						ni := rf.nextIndex[peerIndex]
						prevLogIndex, prevLogTerm := ni-1, int64(-1)
						if prevLogIndex >= 0 {
							prevLogTerm = rf.LogEntries[prevLogIndex].Term
						}
						args := AppendEnriesArgs{
							Term:              rf.CurrentTerm.Load(),
							LeaderId:          rf.me,
							PrevLogIndex:      prevLogIndex,
							PrevLogTerm:       prevLogTerm,
							Entries:           rf.LogEntries[ni:],
							LeaderCommitIndex: rf.commitIndex.Load(),
						}
						reply := AppendEntriesReply{}
						ok := rf.peers[peerIndex].Call("Raft.AppendEntries", &args, &reply)
						if ok {
							fmt.Printf("Leader %d received AppendEntriesReply from peer %d: %v\n", rf.me, peerIndex, reply)
							if reply.Success {
								fmt.Println("AppendEntries succeeded!")
								// update nextIndex and matchIndex for follower
								rf.nextIndex[peerIndex] = int64(len(rf.LogEntries))
								rf.matchIndex[peerIndex] = int64(len(rf.LogEntries) - 1)
								countReplica.Add(1)
								if countReplica.Load() > int64(len(rf.peers)/2) {
									replicateSucceed <- true
								}
								break
							} else {
								if reply.Term > rf.CurrentTerm.Load() {
									fmt.Println("Higher term found, failing back to follower!")
									rf.CurrentTerm.Store(reply.Term)
									rf.state.Store(Follower)
									replicateSucceed <- false
									break
								} else {
									// handle inconsistency: decrement nextIndex and retry
									fmt.Printf("Found inconsistent logs: {prevLogIndex: %d, prevLogTerm: %d, nextIndex: %d, reply.Term: %d}\n", prevLogIndex, prevLogTerm, ni, reply.Term)
									rf.nextIndex[peerIndex]--
								}
							}
						}
						time.Sleep(time.Duration(10) * time.Millisecond)
					}
					fmt.Printf("Appended: {leader: %d, peer: %d, entry: %v}\n", rf.me, peerIndex, entry)
				}(i)
			}
		}
		if <-replicateSucceed {
			fmt.Printf("Agreement achieved: {index: %d, term: %d, isLeader: %v, entry: %v}\n", index, term, isLeader, entry)
			// commit
			rf.mu.Lock()
			l := len(rf.LogEntries)
			rf.mu.Unlock()
			for i := int(rf.commitIndex.Load()) + 1; i < l; i++ {
				msg := ApplyMsg{
					CommandValid: true,
					Command:      rf.LogEntries[i].Command,
					CommandIndex: i + 1, // test start with 1 but we start with 0
				}
				rf.commitIndex.Add(1)
				rf.lastApplied.Add(1)
				fmt.Printf("Leader %d is committing msg: %v, leader.commitIndex: %d, leader.lastApplied: %d\n", rf.me, msg, rf.commitIndex.Load(), rf.lastApplied.Load())
				rf.applyCh <- msg
				fmt.Printf("Leader %d committed msg: %v, leader.commitIndex: %d, leader.lastApplied: %d\n", rf.me, msg, rf.commitIndex.Load(), rf.lastApplied.Load())
			}
		}
	}()

	return int(index), int(term), isLeader
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
	atomic.StoreInt64(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt64(&rf.dead)
	return z == 1
}

// ticker runs as a background go routine to check if this peer needs to
// start an election periodically until it got killed. When election timeout
// elapse, an election starts:
// 1. change its state to candidate;
// 2. increase current term by 1;
// 3. vote it self;
// 4. request votes to all peers via RequestVote RPC;
// 5. becomes leader and sends heartbeats to peers immediately once receiving marjority of votes;
// NOTE:
//  1. this server needs to convert to follower whenever a peer has higher term
//  2. this server shouldn't serve any RequestVote request during its own election to avoid dead lock.
//     The dead lock can happen when two or more servers start election at same time. The server
//     holds the lock during the election, and its RequestVote handler also needs the lock. Two candidates
//     could wait the RequestVote RPC call from each other and hit dead lock, so I set a timeout for the
//     election process.
//
// Server state flow is in Figure 4
func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here (2A)
		// Check if a leader election should be started.
		if rf.shouldStartElection() {
			voteSucceed := make(chan bool)
			// fail the election when it exceeds one second
			go func() {
				time.Sleep(time.Duration(1000) * time.Millisecond)
				voteSucceed <- false
			}()
			rf.mu.Lock()
			rf.state.Store(Candidate)
			rf.CurrentTerm.Add(1)
			fmt.Printf("peer %d starts an election for term %d...\n", rf.me, rf.CurrentTerm.Load())

			// store the current term and check it after the election to ensure that
			// the term is not changed during the election
			termCheck := rf.CurrentTerm.Load()

			voteCount := int64(1)
			rf.VotedFor.Store(rf.me)

			prevLogIndex, prevLogTerm := rf.getPrevLogIndexAndTerm()
			for i := range rf.peers {
				if int64(i) != rf.me {
					// parallelize RPC call to optimize the performance and the server can
					// become leader immediately once it receives a majority of votes
					go func(peerIndex int) {
						args := RequestVoteArgs{
							Term:         rf.CurrentTerm.Load(),
							CandidateId:  rf.me,
							LastLogIndex: prevLogIndex,
							LastLogTerm:  prevLogTerm,
						}
						fmt.Printf("waiting RequestVote from peer %d: %v\n", peerIndex, args)
						reply := RequestVoteReply{}
						ok := rf.peers[peerIndex].Call("Raft.RequestVote", &args, &reply)
						if ok {
							fmt.Printf("got RequestVote reply from peer %d: %v\n", peerIndex, reply)
							if reply.VoteGranted {
								atomic.AddInt64(&voteCount, 1)
								if atomic.LoadInt64(&voteCount) > int64(len(rf.peers)/2) {
									voteSucceed <- true
								}
							} else {
								if reply.Term > rf.CurrentTerm.Load() {
									rf.CurrentTerm.Store(reply.Term)
								}
								voteSucceed <- false
							}
						} else {
							voteSucceed <- false
						}
					}(i)
				}
			}

			if <-voteSucceed && termCheck == rf.CurrentTerm.Load() {
				rf.state.Store(Leader)
				// When a leader first comes to power, it initializes all nextIndex values to the
				// index just after the last one in its log
				for i := range rf.peers {
					rf.nextIndex[i] = int64(len(rf.LogEntries))
				}
				go rf.broadCastHeartbeat()

			}
			fmt.Printf("vote count for candidate %d: %d\n", rf.me, atomic.LoadInt64(&voteCount))
			rf.mu.Unlock()
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) getPrevLogIndexAndTerm() (int64, int64) {
	l := len(rf.LogEntries)
	if l == 0 {
		return -1, -1
	}
	return int64(l - 1), rf.LogEntries[l-1].Term
}

func (rf *Raft) broadCastHeartbeat() {
	fmt.Printf("peer %d is broadcasting heartbeat for term %d...\n", rf.me, rf.CurrentTerm.Load())
	for !rf.killed() && rf.state.Load() == Leader {
		rf.mu.Lock()
		prevLogIndex, prevLogTerm := rf.getPrevLogIndexAndTerm()
		rf.mu.Unlock()
		for i := range rf.peers {
			if int64(i) != rf.me {
				go func(peerIndex int) {
					args := AppendEnriesArgs{
						Term:              rf.CurrentTerm.Load(),
						LeaderId:          rf.me,
						PrevLogIndex:      prevLogIndex,
						PrevLogTerm:       prevLogTerm,
						Entries:           nil,
						LeaderCommitIndex: rf.commitIndex.Load(),
					}
					reply := AppendEntriesReply{}
					ok := rf.peers[peerIndex].Call("Raft.AppendEntries", &args, &reply)
					if ok {
						// fmt.Printf("peer %d sent heartbeat to peer %d in term %d: %v\n", rf.me, peerIndex, rf.CurrentTerm.Load(), reply)
						if !reply.Success && reply.Term > rf.CurrentTerm.Load() {
							rf.state.Store(Follower)
							rf.CurrentTerm.Store(reply.Term)
							fmt.Printf("peer %d is a stale leader, converted to follower and updated to term %d\n", rf.me, rf.CurrentTerm.Load())
						}
					}
				}(i)
			}
		}
		time.Sleep(time.Duration(120) * time.Millisecond)
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
	rf.me = int64(me)
	rf.LogEntries = make([]Entry, 0, 100)
	rf.nextIndex = make([]int64, len(rf.peers))
	rf.matchIndex = make([]int64, len(rf.peers))
	rf.commitIndex.Store(-1)
	rf.lastApplied.Store(-1)
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.lastHeard = time.Now()
	rf.resetElectionTimeout()
	rf.state.Store(Follower)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
