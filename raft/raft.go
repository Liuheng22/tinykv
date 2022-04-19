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
	"errors"
	"math/rand"
	"time"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
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

	randelectionTimeout int

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
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	// Your Code Here (2A).
	// 可能是机器重启？
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	r := &Raft{
		id:                  c.ID,
		Term:                0,
		Vote:                None,
		RaftLog:             newLog(c.Storage),
		Prs:                 make(map[uint64]*Progress),
		State:               StateFollower,
		votes:               make(map[uint64]bool),
		msgs:                make([]pb.Message, 0),
		Lead:                None,
		heartbeatTimeout:    c.HeartbeatTick,
		electionTimeout:     c.ElectionTick,
		heartbeatElapsed:    0,
		electionElapsed:     0,
		randelectionTimeout: 0,
		leadTransferee:      0, //3A
		PendingConfIndex:    0, //3A
	}
	// 初始化和peer相关的状态
	for _, pid := range c.peers {
		r.Prs[pid] = &Progress{}
		r.votes[pid] = false
	}
	// 恢复初始状态？
	if hs, _, err := c.Storage.InitialState(); err == nil {
		r.loadState(hs)
		DPrintf("come here:%v", hs)
	}
	if c.Applied > 0 {
		r.commitApplied(c.Applied)
	}
	// 一些新term的东西需要设置，比如随机时间
	r.becomeFollower(r.Term, None)
	return r
}

// 获取硬状态
func (r *Raft) hardState() pb.HardState {
	hs := pb.HardState{}
	hs.Term = r.Term
	hs.Vote = r.Vote
	hs.Commit = r.RaftLog.committed
	return hs
}

// 获取软状态
func (r *Raft) softState() SoftState {
	ss := SoftState{}
	ss.Lead = r.Lead
	ss.RaftState = r.State
	return ss
}

func (r *Raft) commitApplied(applied uint64) {
	r.RaftLog.applied = applied
}

// 加载原先的HardState
func (r *Raft) loadState(hs pb.HardState) bool {
	if hs.Commit < r.RaftLog.committed || hs.Commit > r.RaftLog.LastIndex() {
		return false
	}
	r.RaftLog.committed = hs.Commit
	r.Term = hs.Term
	r.Vote = hs.Vote
	return true
}

// broadcastAppend
func (r *Raft) broadcastAppend() {
	for id := range r.Prs {
		if id != r.id {
			r.sendAppend(id)
		}
	}
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	process := r.Prs[to]
	msg := pb.Message{MsgType: pb.MessageType_MsgAppend, To: to, From: r.id, Term: r.Term}
	msg.Index = process.Next - 1
	msg.LogTerm, _ = r.RaftLog.Term(msg.Index)
	// 拿到的entry，转换成*entry
	if flag == "copy" || flag == "all" {
		// DPrintf("line 242 {Node: %d} send {Node: %d} from lo: %d to hi: %d", r.id, to, process.Next, r.RaftLog.LastIndex()+1)
	}
	ents := r.RaftLog.findentries(process.Next, r.RaftLog.LastIndex()+1)
	for i := range ents {
		msg.Entries = append(msg.Entries, &ents[i])
	}
	msg.Commit = r.RaftLog.committed
	r.msgs = append(r.msgs, msg)
	if flag == "copy" || flag == "all" {
		DPrintf("{Node %d} in {term: %d} send {Node: %d} {Appendmsg: Idx: %d LogTerm: %d ents: %v} with committed: %d", r.id, r.Term, to, msg.Index, msg.LogTerm, msg.Entries, r.RaftLog.committed)
	}
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	// 发送心跳，带着commit信息
	if flag == "copy" || flag == "all" {
		DPrintf("{Node: %d} send heartbeat to {Node: %d} m.committed: %d", r.id, to, r.RaftLog.committed)
	}
	msg := pb.Message{MsgType: pb.MessageType_MsgHeartbeat, To: to, From: r.id, Term: r.Term, Commit: r.RaftLog.committed}
	r.msgs = append(r.msgs, msg)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower, StateCandidate:
		r.tickElection()
	case StateLeader:
		r.tickHeartbeat()
	}
}

func (r *Raft) tickElection() {
	r.electionElapsed++
	if r.electionElapsed >= r.randelectionTimeout {
		// 超过时间了,开始新的选举
		r.electionElapsed = 0
		m := pb.Message{To: None, MsgType: pb.MessageType_MsgHup, From: r.id}
		r.Step(m)
	}
}
func (r *Raft) tickHeartbeat() {
	// 心跳时间+1
	r.heartbeatElapsed++
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		// 发生心跳
		r.heartbeatElapsed = 0
		m := pb.Message{To: None, MsgType: pb.MessageType_MsgBeat, From: r.id}
		r.Step(m)
	}
}

func (r *Raft) resetrandElectiontimeout() {
	rand.Seed(time.Now().UnixNano())
	r.randelectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
}

// 新的term重置信息
func (r *Raft) reset(term uint64) {
	if r.Term != term {
		r.Term = term
		r.Vote = None
	}
	r.Lead = None
	r.resetrandElectiontimeout()
	r.heartbeatElapsed = 0
	r.electionElapsed = 0

	// 重置选票
	r.votes = make(map[uint64]bool)
	// 重置progress
	lastindex := r.RaftLog.LastIndex()
	for i := range r.Prs {
		r.Prs[i].Match = 0
		r.Prs[i].Next = lastindex + 1
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.reset(term)
	r.Lead = lead
	r.State = StateFollower
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	// 修改Term，投票的
	r.Term = r.Term + 1
	r.reset(r.Term)
	r.Vote = r.id
	r.votes[r.id] = true
	r.State = StateCandidate
}

// 添加entry，更新index和term
func (r *Raft) AppendEntries(ents ...*pb.Entry) {
	lastindex := r.RaftLog.LastIndex()
	for i := range ents {
		ents[i].Term = r.Term
		ents[i].Index = lastindex + 1 + uint64(i)
		r.Prs[r.id].Match = ents[i].Index
		r.Prs[r.id].Next = r.Prs[r.id].Match + 1
	}
	r.RaftLog.AppendEntries(ents...)
	N := r.RaftLog.LastIndex()
	for ; N > r.RaftLog.committed; N-- {
		cnt := 1
		for id := range r.Prs {
			if id != r.id && r.Prs[id].Match >= N {
				cnt++
			}
		}
		if cnt > len(r.Prs)/2 {
			break
		}
	}
	if N != r.RaftLog.committed {
		if flag == "election" || flag == "all" {
			DPrintf("{Node :%d} changed {commited: %d}", r.id, N)
		}
		r.RaftLog.committed = N
		r.broadcastAppend()
	}
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.reset(r.Term)
	r.State = StateLeader
	r.Lead = r.id
	r.AppendEntries(&pb.Entry{})
	N := r.RaftLog.LastIndex()
	for ; N > r.RaftLog.committed; N-- {
		cnt := 1
		for id := range r.Prs {
			if id != r.id && r.Prs[id].Match >= N {
				cnt++
			}
		}
		if cnt > len(r.Prs)/2 {
			break
		}
	}
	if N != r.RaftLog.committed {
		r.RaftLog.committed = N
		r.broadcastAppend()
	}
	if flag == "election" || flag == "all" {
		DPrintf("{Node: %d} become leader in term: %d", r.id, r.Term)
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		r.stepFollower(m)
	case StateCandidate:
		r.stepCandidate(m)
	case StateLeader:
		r.stepLeader(m)
	}
	return nil
}

func (r *Raft) hup() {
	if r.State == StateLeader {
		// 已经是leader，则不需要竞选
		return
	}
	// 如果不是leader，先变成candidate，再选举
	r.becomeCandidate()
	for id := range r.Prs {
		// 给所有人发请求选票的信息
		if id != r.id {
			// 构造requestvote
			msg := pb.Message{}
			msg.MsgType = pb.MessageType_MsgRequestVote
			msg.To = id
			msg.From = r.id
			msg.Term = r.Term
			msg.Index = r.RaftLog.LastIndex()
			msg.LogTerm = r.RaftLog.LastTerm()
			if flag == "election" || flag == "all" {
				DPrintf("{Node: %d} send {Votereq:Term: %d, LogTerm: %d,Index: %d} to {Peer %d} with {state: %v} ", r.id, msg.Term, msg.LogTerm, msg.Index, msg.To, r.State.String())
			}
			r.msgs = append(r.msgs, msg)
		}
	}
	// 若只有一个raft
	granted, reject := r.countVote()
	if granted > len(r.Prs)/2 {
		r.becomeLeader()
		r.broadcastAppend()
	} else if reject > len(r.Prs)/2 {
		r.becomeFollower(r.Term, None)
	}
}

// follower处理消息
func (r *Raft) stepFollower(m pb.Message) {
	switch m.GetMsgType() {
	case pb.MessageType_MsgHup:
		r.hup()
	case pb.MessageType_MsgPropose:
		if r.Lead != None {
			// 如果有别的主，那么转发
			m.To = r.Lead
			r.msgs = append(r.msgs, m)
		}
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgTransferLeader:
		// 转发request
		if r.Lead != None {
			m.To = r.Lead
			r.msgs = append(r.msgs, m)
		}

	case pb.MessageType_MsgTimeoutNow:
		// 开始新的选举
		r.hup()
	}
}

// 统计支持票有多少
func (r *Raft) countVote() (int, int) {
	granted, reject := 0, 0
	for _, v := range r.votes {
		if v == true {
			granted++
		} else {
			reject++
		}
	}
	return granted, reject
}

// candidate
func (r *Raft) stepCandidate(m pb.Message) {
	switch m.GetMsgType() {
	case pb.MessageType_MsgHup:
		r.hup()
	case pb.MessageType_MsgPropose:

	case pb.MessageType_MsgAppend:
		// 收到AppendEntries
		DPrintf("{Node: %d in term:%d state: %v} send {Node: %d in term: %d} m.Index:%d,m.LogTerm:%v,%v", m.From, m.Term, r.State.String(), m.To, r.Term, m.Index, m.LogTerm, r.isLogmatch(m.Index, m.LogTerm))
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:

	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		if m.Term == r.Term {
			// 返回的相应Term的
			r.votes[m.From] = !m.Reject
			if flag == "election" || flag == "all" {
				DPrintf("{Node %d} receives RequestVoteResp from {Peer %d} with votes %v with {state: %v}", r.id, m.From, r.votes, r.State.String())
			}
			granted, reject := r.countVote()
			// 超过一半的话，转为leader
			if granted > len(r.Prs)/2 {
				r.becomeLeader()
				r.broadcastAppend()
			} else if reject > len(r.Prs)/2 {
				r.becomeFollower(r.Term, None)
			}
		} else if m.Term > r.Term {
			// 如果回复的Term比较大，那么就转为follower
			r.becomeFollower(m.Term, None)
		}
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgTransferLeader:
		// 转发request
		if r.Lead != None {
			m.To = r.Lead
			r.msgs = append(r.msgs, m)
		}

	case pb.MessageType_MsgTimeoutNow:
		// 开始新的选举
		r.hup()
	}
}

// leader
func (r *Raft) stepLeader(m pb.Message) {
	switch m.GetMsgType() {
	case pb.MessageType_MsgHup:
		r.hup()
	case pb.MessageType_MsgBeat:
		// 发送heartbeat给所有的peer
		for pid := range r.Prs {
			if pid != r.id {
				r.sendHeartbeat(pid)
			}
		}
	case pb.MessageType_MsgPropose:
		// 提交entry,先给leader，再发给所有的
		// 当前leader在转换？
		// 先给自己添加entries
		// 再给所有的peer发送
		r.AppendEntries(m.Entries...)
		r.broadcastAppend()
	case pb.MessageType_MsgAppend:
		// leader也会收到Append消息
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendResponse(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:

	}
}

// 处理appendResponse
func (r *Raft) handleAppendResponse(m pb.Message) {
	// 收到消息
	if flag == "copy" || flag == "all" {
		DPrintf("{Node: %d} receive appeendresp from {peer %d} in {term : %d}with {state: %v}", r.id, m.From, m.Term, r.State.String())
	}
	if m.Term > r.Term {
		// 对方Term比自己大
		r.becomeFollower(m.Term, None)
	} else {
		// 收到是和自己相同的term
		// 是成功还是失败？
		// 找到对应progress
		matchindex := m.Index
		progress := r.Prs[m.From]
		// 如果拒绝，并且返回的logTerm非None
		// 则可以往前探测
		if m.Reject && m.LogTerm != None {
			// 拒绝了
			matchindex, _ = r.RaftLog.Findconflictbyterm(m.Index, m.LogTerm)
		}
		if m.Reject {
			progress.Next = min(matchindex+1, progress.Next-1)
		} else {
			progress.Next = matchindex + 1
			progress.Match = matchindex
		}
		N := r.RaftLog.LastIndex()
		for ; N > r.RaftLog.committed; N-- {
			cnt := 1
			for id := range r.Prs {
				if id != r.id && r.Prs[id].Match >= N {
					cnt++
				}
			}
			if cnt > len(r.Prs)/2 {
				break
			}
		}
		committerm, _ := r.RaftLog.Term(N)
		if N != r.RaftLog.committed && committerm == r.Term {
			// 只提交当前term的log
			r.RaftLog.committed = N
			r.broadcastAppend()
		}
		lastindex := r.RaftLog.LastIndex()
		if progress.Next <= lastindex {
			r.sendAppend(m.From)
		}
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	// 收到了append请求
	// DPrintf("line 581 {Node: %d in term:%d } send {Node: %d in term: %d state: %v} %v,%v,%v", m.From, m.Term, m.To, r.Term, r.State.String(), m.Index, m.LogTerm, r.isLogmatch(m.Index, m.LogTerm))

	if r.Term > m.Term {
		// 如果term比leader大，则拒绝
		msg := pb.Message{MsgType: pb.MessageType_MsgAppendResponse, To: m.From, From: r.id, Term: r.Term, Reject: true}
		r.msgs = append(r.msgs, msg)
		if flag == "copy" || flag == "all" {
			DPrintf("{Node %d} send {AppendResp: Term: %d, Reject: %v} to {peer: %d} in {term : %d} with {state: %v}", r.id, msg.Term, msg.Reject, m.From, m.Term, r.State.String())
		}
		return
	}

	// leader的Term比我大，变成follower
	// 修改lead为m.From
	r.becomeFollower(m.Term, m.From)

	// 如果发送的message的消息早于commit，则这个消息应该拒绝，因为commit的entry不应该修改
	// 回复的Index应该是已经匹配的Index
	if m.Index < r.RaftLog.committed {
		msg := pb.Message{MsgType: pb.MessageType_MsgAppendResponse, To: m.From, From: r.id, Term: r.Term}
		msg.Index = r.RaftLog.committed
		msg.LogTerm = None
		r.msgs = append(r.msgs, msg)
		if flag == "copy" || flag == "all" {
			DPrintf("{Node %d} send {AppendResp: Term: %d,LogTerm: %d,Index: %d Reject: %v} to {peer: %d} in {term : %d} with {state: %v}", r.id, msg.Term, msg.LogTerm, msg.Index, msg.Reject, m.From, m.Term, r.State.String())
		}
		return
	}
	DPrintf("{Node: %d in term:%d} send {Node: %d in term: %d} %v,%v,%v", m.From, m.Term, m.To, r.Term, m.Index, m.LogTerm, m.Entries)
	if !r.isLogmatch(m.Index, m.LogTerm) {
		msg := pb.Message{MsgType: pb.MessageType_MsgAppendResponse, To: m.From, From: r.id, Term: r.Term, Reject: true}
		hintindex := min(m.Index, r.RaftLog.LastIndex())
		hintindex, hintterm := r.RaftLog.Findconflictbyterm(hintindex, m.LogTerm)
		if hintterm == None {

		}
		msg.Index = hintindex
		msg.LogTerm = hintterm
		r.msgs = append(r.msgs, msg)
		if flag == "copy" || flag == "all" {
			DPrintf("{Node %d} send {AppendResp: Term: %d,LogTerm: %d,Index: %d Reject: %v} to {peer: %d} in {term : %d} with {state: %v}", r.id, msg.Term, msg.LogTerm, msg.Index, msg.Reject, m.From, m.Term, r.State.String())
		}
		return
	}
	// 匹配的log，则认为有leader了
	// 更新committed
	r.handleEntries(m.Entries...)
	lastindex := m.Index + uint64(len(m.Entries))
	r.RaftLog.committed = min(lastindex, m.Commit)
	msg := pb.Message{MsgType: pb.MessageType_MsgAppendResponse, To: m.From, From: r.id, Term: r.Term, Reject: false}
	// 成功的话，返回index+1，作为下一轮的nextIndex
	msg.Index = r.RaftLog.LastIndex()
	r.msgs = append(r.msgs, msg)
	if flag == "copy" || flag == "all" {
		DPrintf("{Node %d} send {AppendResp: Term: %d,LogTerm: %d,Index: %d Reject: %v} to {peer: %d} in {term : %d} with {state: %v}", r.id, msg.Term, msg.LogTerm, msg.Index, msg.Reject, m.From, m.Term, r.State.String())
	}
}

// 处理添加的日志
func (r *Raft) handleEntries(ents ...*pb.Entry) {
	var comflictindex uint64 = None
	for _, e := range ents {
		if !r.isLogmatch(e.Index, e.Term) {
			comflictindex = e.Index
			break
		}
	}
	if comflictindex != None {
		// 有冲突，写进log
		start := comflictindex - ents[0].Index

		r.RaftLog.AppendEntries(ents[start:]...)
	}
}

// 日志匹配
func (r *Raft) isLogmatch(index uint64, term uint64) bool {
	logterm, _ := r.RaftLog.Term(index)
	return logterm == term
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	if r.Term > m.Term {
		msg := pb.Message{MsgType: pb.MessageType_MsgHeartbeatResponse, To: m.From, From: r.id, Term: r.Term}
		r.msgs = append(r.msgs, msg)
		if flag == "copy" || flag == "all" {
			DPrintf("{Node: %d} send {heartbeatResp:Term: %d} to {Peer %d} in term: %d with {state: %v} ", r.id, msg.Term, m.From, m.Term, r.State.String())
		}
		return
	}
	r.becomeFollower(m.Term, m.From)
	r.RaftLog.committed = m.Commit
	msg := pb.Message{MsgType: pb.MessageType_MsgHeartbeatResponse, To: m.From, From: r.id, Term: r.Term}
	r.msgs = append(r.msgs, msg)
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// handleRequestVote
func (r *Raft) handleRequestVote(m pb.Message) {
	// 收到了投票请求
	if r.Term > m.Term {
		// 如果term比候选者大，则拒绝
		msg := pb.Message{MsgType: pb.MessageType_MsgRequestVoteResponse, To: m.From, From: r.id, Term: r.Term, Reject: true}
		r.msgs = append(r.msgs, msg)
		if flag == "election" || flag == "all" {
			DPrintf("{Node: %d} send {requestResp:Term: %d, Reject: %v} to {Peer %d} in term: %d with {state: %v} ", r.id, msg.Term, msg.Reject, m.From, m.Term, r.State.String())
		}
		return
	}
	if r.Term < m.Term {
		// 对方Term比我大，变成follower
		r.becomeFollower(m.Term, None)
	}
	// 因为前面的操作，所以现在当前raft的term一定是与m的term一样的
	// 因为前面的设置，可能出现vote为None，而lead不为None，这个时候也不能投票
	if ((r.Vote == None && r.Lead == None) || r.Vote == m.From) && r.RaftLog.isUpToDate(m.Index, m.LogTerm) {
		msg := pb.Message{MsgType: pb.MessageType_MsgRequestVoteResponse, To: m.From, From: r.id, Term: r.Term, Reject: false}
		r.msgs = append(r.msgs, msg)
		// 投票了，那么就要重置竞选时间
		r.Vote = m.From
		r.electionElapsed = 0
		if flag == "election" || flag == "all" {
			DPrintf("{Node: %d} send {requestResp:Term: %d, Reject: %v} to {Peer %d} in term: %d with {state: %v} ", r.id, msg.Term, msg.Reject, m.From, m.Term, r.State.String())
		}
	} else {
		msg := pb.Message{MsgType: pb.MessageType_MsgRequestVoteResponse, To: m.From, From: r.id, Term: r.Term, Reject: true}
		r.msgs = append(r.msgs, msg)
		if flag == "election" || flag == "all" {
			DPrintf("{Node: %d} send {requestResp:Term: %d, Reject: %v} to {Peer %d} in term: %d with {state: %v} ", r.id, msg.Term, msg.Reject, m.From, m.Term, r.State.String())
		}
	}
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
