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
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pkg/errors"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).

	// entries to be stabled
	EntriesStable []pb.Entry

	// entries to be committed
	EntriesCommit []pb.Entry
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	lastIndex, err := storage.LastIndex()
	if err != nil {
		return nil
	}
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		return nil
	}

	var entries []pb.Entry
	entriesFromStorage, _ := storage.Entries(firstIndex, lastIndex + 1)
    if entriesFromStorage != nil {
    	entries = entriesFromStorage
	} else {
		entries = make([]pb.Entry, 0)
	}

	hardState, _, err := storage.InitialState()
	committed := hardState.Commit
	applied := firstIndex - 1
	stabled := lastIndex
	entriesStable := []pb.Entry{}
	entriesCommit := []pb.Entry{}
	if committed != 0 {
		entriesCommit = entries[:committed - firstIndex + 1]
	}
	return &RaftLog{
		storage: storage,
		committed: committed,
		applied: applied,
		stabled: stabled,
		entries: entries,
		EntriesStable: entriesStable,
		EntriesCommit: entriesCommit,
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	offset := l.getDummyIndex() + 1
	return l.entries[l.stabled + 1 - offset: ]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	offset := l.getDummyIndex() + 1
	return l.entries[l.applied + 1 - offset : l.committed - offset + 1]
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	return l.getDummyIndex() + uint64(len(l.entries))
}

// FirstIndex return the first index of the log entries
func (l *RaftLog) FirstIndex() uint64 {
	// Your Code Here (2A).
	return l.getDummyIndex() + 1
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if i < l.FirstIndex() - 1 {
		return 0, errors.New("index provided is smaller than the available index " + string(l.FirstIndex() - 1))
	} else if i > l.LastIndex() {
		return 0, errors.New("index provided is larger than the available index " + string(l.LastIndex()))
	} else if i == l.FirstIndex() - 1 {
		return l.getDummyTerm(), nil
	}
	offset := l.getDummyIndex() + 1
	return l.entries[i - offset].Term, nil
}

func (l *RaftLog) LastTerm() uint64 {
	lastTerm, _ := l.Term(l.LastIndex())
	return lastTerm
}

func (l *RaftLog) getEntries(lo uint64, hi uint64) ([]pb.Entry) {
	var offset uint64

	offset = l.getDummyIndex() + 1
	if lo < offset {
		return nil
	}
	if hi > l.LastIndex() +1 {
		return nil
	}

	return l.entries[lo-offset : hi-offset]
}

// get index of dummy entry in storage
func (l *RaftLog) getDummyIndex() uint64 {
	firstIndex, _ := l.storage.FirstIndex()
	return firstIndex - 1
}

// get term of dummy entry in storage
func (l *RaftLog) getDummyTerm() uint64 {
	firstIndex, _ := l.storage.FirstIndex()
	term, _ := l.storage.Term(firstIndex - 1)
	return term
}

