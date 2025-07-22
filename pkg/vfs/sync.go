package vfs

import (
	"fmt"
	"sync"
	"sync/atomic"
)

const (
	SYNC_LOCK_READ = 1 << iota
	SYNC_EXCLUSIVE_WRITE
)

type hasOnRevoked interface {
	OnRevoked(lastFlag int)
}

type operation struct {
	readonly     bool // Indicates if the operation is read-only
	async        bool
	startedAsync bool
	generation   int64 // Used to track the generation of the operation
}

func (o *operation) Async() bool {
	return o.async
}

type syncer struct {
	flags        int
	desiredFlags int
	mu           sync.Mutex
	cond         *sync.Cond // Used to signal when claim status changes
	operations   operation  // List of operations that are pending or in progress.
	writerWait   int
	writerActive bool
	readers      int
	asyncOps     int
	generation   atomic.Int64 // Used to track the generation of the syncer

	// Notify the Inode to
	// 1. Clear current cached data
	// 2. Remove from future server updates.
	notif hasOnRevoked
}

func NewSyncer() *syncer {
	n := &syncer{}
	n.cond = sync.NewCond(&n.mu)
	return n
}

func (s *syncer) StartWrite() *operation {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.writerWait++
	for {
		pendingClaimUpgrade := (s.desiredFlags & (s.flags ^ s.desiredFlags) & SYNC_EXCLUSIVE_WRITE) != 0
		pendingRevoke := s.flags&(s.flags^s.desiredFlags) != 0
		hasSyncAccess := ((s.writerActive || s.readers > 0) && s.flags&SYNC_EXCLUSIVE_WRITE != 0)
		hasReader := (s.readers > 0 && s.flags&SYNC_LOCK_READ != 0)
		if pendingClaimUpgrade || pendingRevoke || hasSyncAccess || hasReader {
			s.cond.Wait()
		} else {
			break
		}
	}
	s.writerActive = true
	s.writerWait--
	// Self revoke the read claim as modification happens.
	if s.flags&SYNC_LOCK_READ != 0 || s.desiredFlags&SYNC_LOCK_READ != 0 {
		s.flags &^= SYNC_LOCK_READ
		s.desiredFlags &^= SYNC_LOCK_READ
		s.cond.Broadcast()
	}
	op := &operation{
		readonly: false,
		async:    s.flags&SYNC_EXCLUSIVE_WRITE != 0,
	}
	return op
}

func (s *syncer) StartAsync(op *operation) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !op.async {
		panic("Operation is not async")
	}
	if op.startedAsync {
		panic("Operation already started async")
	}
	op.startedAsync = true
	s.asyncOps++
}

func (s *syncer) StartRead() *operation {
	s.mu.Lock()
	defer s.mu.Unlock()
	for {
		pendingClaimUpdate := s.flags != s.desiredFlags
		hasSyncWriter := (s.writerActive || s.writerWait > 0) && s.flags&SYNC_EXCLUSIVE_WRITE != 0
		// Wait until no pending grant, and if cache is valid, wait for the writer to finish
		if pendingClaimUpdate || hasSyncWriter {
			s.cond.Wait()
		} else {
			break
		}
	}
	op := &operation{
		readonly: true,
		async:    s.flags&(SYNC_EXCLUSIVE_WRITE|SYNC_LOCK_READ) != 0,
	}
	s.readers++
	return op
}

// Called by every operation. For async operations, this will be called after the operation is added to the queue.
func (s *syncer) Complete(op *operation) {
	if op == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if op.readonly {
		s.readers--
	} else {
		s.writerActive = false
	}
	s.checkClaimUpdates()
	s.cond.Broadcast()
}

// Called when async write operations are completed.
func (s *syncer) CompleteAsync(op *operation) {
	if op == nil {
		return
	}
	if !op.startedAsync {
		panic("CompleteAsync called on a non-async operation")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.asyncOps--
	if s.checkClaimUpdates() {
		s.cond.Broadcast()
	}
}

func (s *syncer) UpgradeClaim(mask, newFlag int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.desiredFlags = s.flags & ^mask | (newFlag & mask)
	// Wait for all operations to complete before upgrading the claim
	if s.checkClaimUpdates() {
		s.cond.Broadcast()
	}
}

func (s *syncer) CanCache(op *operation) bool {
	return op.async && op.generation == s.generation.Load()
}

func (s *syncer) checkClaimUpdates() bool {
	if s.flags != s.desiredFlags && s.emptyLocked() {
		debugf("%p Claim update: %b -> %b", s, s.flags, s.desiredFlags)
		if s.notif != nil && s.desiredFlags == 0 {
			s.generation.Add(1)
			s.notif.OnRevoked(s.flags)
		}
		s.flags = s.desiredFlags
		return true
	}
	return false
}

func (s *syncer) emptyLocked() bool {
	return s.readers == 0 && !s.writerActive && s.asyncOps == 0
}

func (s *syncer) SetOnRevoked(callback hasOnRevoked) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.notif = callback
}

func (s *syncer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return fmt.Sprintf("Syncer{flags: %b, desiredFlags: %b, writerWait: %d, writerActive: %t, readers: %d, asyncOps: %d}",
		s.flags, s.desiredFlags, s.writerWait, s.writerActive, s.readers, s.asyncOps)
}
