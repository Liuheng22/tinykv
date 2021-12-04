// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"crypto/rand"
	"errors"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"math/big"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

const Max_ElectionTimeout = 20
const Min_ElectionTimeout = 1

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// reject records
	reject map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

	electionTick int

}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).

	votes := make(map[uint64]bool)
	reject := make(map[uint64]bool)
	prs := make(map[uint64]*Progress)
	for _, id := range c.peers {
		votes[id] = false
		prs[id] = &Progress{}
		reject[id] = false
	}
	electionTimeout, _ := rand.Int(rand.Reader, big.NewInt(int64(c.ElectionTick)))
	//lastIndex, _ := c.Storage.LastIndex()
	//term, _ := c.Storage.Term(lastIndex)
	initialState, _, _ := c.Storage.InitialState()
	return &Raft{
		id: c.ID,
		Term: initialState.Term,
		Vote: initialState.Vote,
		RaftLog: newLog(c.Storage),
		State: StateFollower,
		heartbeatTimeout: c.HeartbeatTick,
		electionTick: c.ElectionTick,
		electionTimeout: int(electionTimeout.Int64()) + c.ElectionTick,
		heartbeatElapsed: 0,
		electionElapsed: 0,
		votes: votes,
		reject: reject,
		Prs: prs,
	}
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	offset := r.RaftLog.entries[0].Index
	lo := r.Prs[to].Next - offset
	hi := r.RaftLog.LastIndex() - offset + 1
	entries := make([]*pb.Entry, hi-lo)
	for i := lo; i < hi; i ++ {
		entries[i-lo] = &r.RaftLog.entries[i]
	}
	if len(entries) == 0 {
		entries = []*pb.Entry{}
	}
	logTerm, _ := r.RaftLog.Term(r.Prs[to].Next - 1)
	r.msgs = append(r.msgs, pb.Message{
		From: r.id,
		To: to,
		Term: r.Term,
		LogTerm: logTerm,
		Index: r.Prs[to].Next - 1,
		Entries: entries,
		Commit: r.RaftLog.committed,
		MsgType: pb.MessageType_MsgAppend,
	})

	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	r.msgs = append(r.msgs, pb.Message{
		From: r.id,
		To: to,
		Term: r.Term,
		Commit: r.RaftLog.committed,
		MsgType: pb.MessageType_MsgHeartbeat,
	})
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	if r.State == StateFollower {
		r.electionElapsed ++
		if r.electionTimeout == r.electionElapsed {
			r.becomeCandidate()
			for id, _ := range r.votes {
				if id != r.id {
					r.msgs = append(r.msgs, pb.Message{
						From: r.id,
						To: id,
						Term: r.Term,
						Index: r.RaftLog.LastIndex(),
						LogTerm: r.RaftLog.LastTerm(),
						MsgType: pb.MessageType_MsgRequestVote,
					})
				}
			}
		}
	} else if r.State == StateCandidate {
		r.electionElapsed ++
		if r.electionTimeout == r.electionElapsed {
			r.becomeCandidate()
			for id, _ := range r.votes {
				if id != r.id {
					r.msgs = append(r.msgs, pb.Message{
						From: r.id,
						To: id,
						Term: r.Term,
						Index: r.RaftLog.LastIndex(),
						LogTerm: r.RaftLog.LastTerm(),
						MsgType: pb.MessageType_MsgRequestVote,
					})
				}
			}
		}
	} else if r.State == StateLeader {
		r.electionElapsed ++
		r.heartbeatElapsed ++
		if r.heartbeatTimeout == r.heartbeatElapsed {
			r.heartbeatElapsed = 0
			for id, _ := range r.votes {
				if id != r.id {
					r.sendHeartbeat(id)
				}
			}
		}
	}
}

func (r *Raft) isElected() bool {
	getVotes := 0
	for _, vote := range r.votes {
		if vote == true {
			getVotes ++
		}
	}
	if getVotes > len(r.votes) / 2 {
		return true
	}
	return false
}

func (r *Raft) isFailed() bool {
	getReject := 0
	for _, reject := range r.reject {
		if reject == true {
			getReject ++
		}
	}
	if getReject > len(r.reject) / 2 {
		return true
	}
	return false
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.Term = term
	r.Lead = lead
	r.Vote = 0
	for id, _ := range r.votes {
		r.votes[id] = false
	}
	for id, _ := range r.reject {
		r.reject[id] = false
	}
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	r.Term ++
	r.Vote = r.id
	r.votes[r.id] = true
	r.electionElapsed = 0
	if r.isElected() {
		r.becomeLeader()
	}
	electionTimeout, _ := rand.Int(rand.Reader, big.NewInt(int64(r.electionTick)))
	r.electionTimeout = int(electionTimeout.Int64()) + r.electionTick
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.Lead = r.id
	r.heartbeatElapsed = 0
	for _, prs := range r.Prs {
		prs.Next = r.RaftLog.LastIndex() + 1
		prs.Match = 0
	}
	r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{
		Index: r.RaftLog.LastIndex() + 1,
		Term: r.Term,
	})
	r.RaftLog.EntriesStable = r.RaftLog.unstableEntries()
	if len(r.Prs) == 1 {
		r.RaftLog.EntriesCommit = r.RaftLog.getEntries(r.RaftLog.committed + 1, r.RaftLog.LastIndex() + 1)
		r.RaftLog.committed = r.RaftLog.LastIndex()
	}
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
	for id, _ := range r.Prs {
		if id == r.id {
			continue
		}
		r.sendAppend(id)
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	for {
		switch r.State {
		case StateFollower:
			if r.Term < m.Term {
				//r.becomeFollower(m.Term, None)
				r.Term = m.Term
				r.Vote = 0
			} else if r.Term > m.Term && m.Term != 0 {
				return nil
			}
			if m.MsgType == pb.MessageType_MsgRequestVote {
				if r.Vote != 0 && m.From != r.Vote ||
					r.RaftLog.LastTerm() > m.LogTerm ||
					r.RaftLog.LastTerm() == m.LogTerm && r.RaftLog.LastIndex() > m.Index{
					r.msgs = append(r.msgs, pb.Message{
						From: r.id,
						To: m.From,
						Term: m.Term,
						MsgType: pb.MessageType_MsgRequestVoteResponse,
						Reject: true,
					})
				} else {
					r.Vote = m.From
					r.msgs = append(r.msgs, pb.Message{
						From: r.id,
						To: m.From,
						Term: m.Term,
						MsgType: pb.MessageType_MsgRequestVoteResponse,
						Reject: false,
					})
				}
				return nil
			} else if m.MsgType == pb.MessageType_MsgHup {
				r.becomeCandidate()
				for id, _ := range r.votes {
					if id != r.id {
						r.msgs = append(r.msgs, pb.Message{
							From: r.id,
							To: id,
							Term: r.Term,
							LogTerm: r.RaftLog.LastTerm(),
							Index: r.RaftLog.LastIndex(),
							MsgType: pb.MessageType_MsgRequestVote,
						})
					}
				}
				return nil
			} else if m.MsgType == pb.MessageType_MsgHeartbeat {
				r.handleHeartbeat(m)
				return nil
			} else if m.MsgType == pb.MessageType_MsgAppend {
				r.handleAppendEntries(m)
				return nil
			} else{
				return nil
			}
		case StateCandidate:
			if r.Term < m.Term {
				r.becomeFollower(m.Term, None)
				break
			} else if r.Term > m.Term && m.Term != 0  {
				return nil
			}
			if m.MsgType == pb.MessageType_MsgRequestVote {
				r.msgs = append(r.msgs, pb.Message{
					From: r.id,
					To: m.From,
					Term: m.Term,
					MsgType: pb.MessageType_MsgRequestVoteResponse,
					Reject: true,
				})
				return nil
			} else if m.MsgType == pb.MessageType_MsgRequestVoteResponse {
				if !m.Reject {
					r.votes[m.From] = true
					if r.isElected() {
						r.becomeLeader()
					}
				} else {
					r.reject[m.From] = true
					if r.isFailed() {
						r.becomeFollower(m.Term, 0)
					}
				}
				return nil
			} else if m.MsgType == pb.MessageType_MsgHeartbeat{
				if m.From != r.id {
					r.becomeFollower(m.Term, m.From)
					break
				}
			} else if m.MsgType == pb.MessageType_MsgAppend {
				if m.From != r.id {
					r.becomeFollower(m.Term, m.From)
					break
				}
			} else if m.MsgType == pb.MessageType_MsgHup {
				r.becomeCandidate()
				for id, _ := range r.votes {
					if id != r.id {
						r.msgs = append(r.msgs, pb.Message{
							From: r.id,
							To: id,
							Term: r.Term,
							LogTerm: r.RaftLog.LastTerm(),
							Index: r.RaftLog.LastIndex(),
							MsgType: pb.MessageType_MsgRequestVote,
						})
					}
				}
				return nil
			} else {
				return nil
			}
		case StateLeader:
			if r.Term < m.Term {
				r.becomeFollower(m.Term, None)
				break
			} else if r.Term > m.Term && m.Term != 0 {
				return nil
			}
			if m.MsgType == pb.MessageType_MsgRequestVote {
				r.msgs = append(r.msgs, pb.Message{
					From: r.id,
					To: m.From,
					Term: m.Term,
					MsgType: pb.MessageType_MsgRequestVoteResponse,
					Reject: true,
				})
				return nil
			} else if m.MsgType == pb.MessageType_MsgRequestVoteResponse {
				if !m.Reject {
					r.votes[m.From] = true
				} else {
					r.reject[m.From] = true
				}
				return nil
			} else if m.MsgType == pb.MessageType_MsgBeat {
				for id, _ := range r.votes {
					if id != r.id {
						r.sendHeartbeat(id)
					}
				}
				return nil
			} else if m.MsgType == pb.MessageType_MsgPropose {
				for _, entryPtr := range m.Entries {
					(*entryPtr).Index = r.RaftLog.LastIndex() + 1
					(*entryPtr).Term = r.Term
					r.RaftLog.entries = append(r.RaftLog.entries, *entryPtr)
				}
				r.Prs[r.id].Match = r.RaftLog.LastIndex()
				r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
				for id, _ := range r.Prs {
					if id == r.id {
						continue
					}
					r.sendAppend(id)
				}
				if len(r.Prs) == 1 {
					/*err := r.RaftLog.storage.Append(r.RaftLog.unstableEntries())
					if err != nil {
						return err
					}
					r.RaftLog.stabled = r.RaftLog.LastIndex()
					r.RaftLog.committed = r.RaftLog.stabled*/
					r.RaftLog.EntriesStable = r.RaftLog.unstableEntries()
					r.RaftLog.EntriesCommit = r.RaftLog.getEntries(r.RaftLog.committed + 1, r.RaftLog.LastIndex() + 1)
					r.RaftLog.committed = r.RaftLog.LastIndex()
				}

				return nil
			} else if m.MsgType == pb.MessageType_MsgAppendResponse {
				if !m.Reject {
					r.Prs[m.From].Match = m.Index
					r.Prs[m.From].Next = m.Index + 1
					term, _ := r.RaftLog.Term(m.Index)
					if term == r.Term {
						// new commit
						if m.Index > r.RaftLog.committed {
							count := 1
							for id, prs := range r.Prs {
								if id == r.id {
									continue
								}
								if prs.Match >= m.Index {
									count ++
								}
							}
							if count > len(r.Prs) / 2 {
								/*err := r.RaftLog.storage.Append(r.RaftLog.unstableEntries())
								if err != nil {
									return err
								}
								r.RaftLog.stabled = m.Index
								r.RaftLog.committed = r.RaftLog.stabled*/
								r.RaftLog.EntriesStable = r.RaftLog.unstableEntries()
								r.RaftLog.EntriesCommit = r.RaftLog.getEntries(r.RaftLog.committed + 1, m.Index + 1)
								r.RaftLog.committed = m.Index
								for id, _ := range r.Prs {
									if id == r.id {
										continue
									}
									r.sendAppend(id)
								}
							}
						}
					}
				} else {
					r.Prs[m.From].Next --
				}
				if r.Prs[m.From].Match < r.RaftLog.LastIndex() {
					r.sendAppend(m.From)
				}

                return nil
			} else if m.MsgType == pb.MessageType_MsgHeartbeatResponse {
				if r.Prs[m.From].Match < r.RaftLog.LastIndex() {
					r.sendAppend(m.From)
				}

				return nil
			} else {
				return nil
			}
		}
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	r.Lead = m.From
	term, _ := r.RaftLog.Term(m.Index)
	// preIndex and preLogTerm is matched
	if term == m.LogTerm {
		r.msgs = append(r.msgs, pb.Message{
			From: r.id,
			To: m.From,
			Term: m.Term,
			MsgType: pb.MessageType_MsgAppendResponse,
			Index: m.Index + uint64(len(m.Entries)),
			Reject: false,
		})

		for i := 0; i < len(m.Entries); i++ {
			offset := r.RaftLog.getDummyIndex() + 1
			indexCurrent := m.Index + 1 + uint64(i)
			if indexCurrent - offset < uint64(len(r.RaftLog.entries)) {
				if m.Entries[i].Term != r.RaftLog.entries[indexCurrent - offset].Term {
					r.RaftLog.entries = r.RaftLog.entries[:indexCurrent - offset]
					if r.RaftLog.stabled > m.Index + uint64(i) {
						r.RaftLog.stabled = m.Index + uint64(i)
					}
					r.RaftLog.entries = append(r.RaftLog.entries, *m.Entries[i])
				}
			} else {
				r.RaftLog.entries = append(r.RaftLog.entries, *m.Entries[i])
			}
		}

		if m.Commit != 0 {
			commit := min(m.Index + uint64(len(m.Entries)), m.Commit)
			if r.RaftLog.stabled < commit {
				/*lo := r.RaftLog.stabled + 1
				hi := commit + 1
				err := r.RaftLog.storage.Append(r.RaftLog.getEntries(lo, hi))
				if err != nil {
					errors.New(err.Error())
				}
				r.RaftLog.stabled = commit
				r.RaftLog.committed = r.RaftLog.stabled*/
				lo := r.RaftLog.stabled + 1
				hi := commit + 1
				r.RaftLog.EntriesStable = r.RaftLog.getEntries(lo, hi)
				r.RaftLog.EntriesCommit = r.RaftLog.getEntries(r.RaftLog.committed + 1, commit + 1)
				r.RaftLog.committed = commit
			} else{
				r.RaftLog.EntriesCommit = r.RaftLog.getEntries(r.RaftLog.committed + 1, commit + 1)
				r.RaftLog.committed = commit
			}
			//r.RaftLog.committed = commit
		}
	// preIndex and preLogTerm is not matched
	} else {
		r.msgs = append(r.msgs, pb.Message{
			From: r.id,
			To: m.From,
			Term: m.Term,
			MsgType: pb.MessageType_MsgAppendResponse,
			Index: m.Index + uint64(len(m.Entries)),
			Reject: true,
		})
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	r.Lead = m.From
	r.electionTimeout = 0
	r.msgs = append(r.msgs, pb.Message{
		From: r.id,
		To: m.From,
		Term: r.Term,
		MsgType: pb.MessageType_MsgHeartbeatResponse,
	})
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
