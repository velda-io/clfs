package vfs

import (
	"sync"
)

const (
	SYNC_EXCLUSIVE_WRITE = 1 << iota
	SYNC_LOCK_READ
	SYNC_EXCLUSIVE_WRITE_GRANTED
	SYNC_LOCK_READ_GRANTED
)

type operationStatus int

const (
	STATUS_PENDING operationStatus = iota
	STATUS_SENT
	STATUS_COMPLETED
)

type operation struct {
	readonly bool // Indicates if the operation is read-only
	async    bool
}

func (o *operation) Async() bool {
	return o.async
}

type syncer struct {
	flags        int
	mu           sync.Mutex
	cond         *sync.Cond // Used to signal when claim status changes
	operations   operation  // List of operations that are pending or in progress.
	writerWait   int
	writerActive bool
	readers      int
	asyncOps     int
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
	for s.flags&SYNC_EXCLUSIVE_WRITE_GRANTED != 0 ||
		((s.writerActive || s.readers > 0) && s.flags&SYNC_EXCLUSIVE_WRITE != 0) ||
		(s.readers > 0 && s.flags&SYNC_LOCK_READ != 0) {
		s.cond.Wait()
	}
	s.writerActive = true
	s.writerWait--
	// Self revoke the read grant as modification happens.
	if s.flags&(SYNC_LOCK_READ|SYNC_LOCK_READ_GRANTED) != 0 {
		s.flags &^= (SYNC_LOCK_READ_GRANTED | SYNC_LOCK_READ)
		s.cond.Broadcast()
	}
	op := &operation{
		readonly: false,
		async:    s.flags&SYNC_EXCLUSIVE_WRITE != 0,
	}
	if op.async {
		s.asyncOps++
	}
	return op
}

func (s *syncer) StartRead() *operation {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Wait until no pending grant, and if cache is valid, wait for the writer to finish
	for s.flags&(SYNC_EXCLUSIVE_WRITE_GRANTED|SYNC_LOCK_READ_GRANTED) != 0 ||
		((s.writerActive || s.writerWait > 0) && s.flags&(SYNC_EXCLUSIVE_WRITE|SYNC_LOCK_READ) != 0) {
		s.cond.Wait()
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
	s.cond.Broadcast()
}

// Called when async operations are completed.
func (s *syncer) CompleteAsync(op *operation) {
	if op == nil {
		return
	}
	if !op.async {
		panic("CompleteAsync called on a non-async operation")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.asyncOps--
	if s.asyncOps == 0 {
		s.cond.Broadcast()
	}
}

func (s *syncer) UpgradeClaim(newFlag int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.flags&SYNC_EXCLUSIVE_WRITE != 0 {
		return // Cannot upgrade if already holding a write or read claim
	}
	if s.flags&SYNC_LOCK_READ != 0 && newFlag&SYNC_EXCLUSIVE_WRITE_GRANTED == 0 {
		return // Already have a read claim.
	}
	s.flags |= newFlag
	// Wait for all operations to complete before upgrading the claim
	for (!s.emptyLocked() || s.asyncOps > 0) && s.flags&(SYNC_EXCLUSIVE_WRITE_GRANTED|SYNC_LOCK_READ_GRANTED) != 0 {
		s.cond.Wait()
	}
	if s.flags&SYNC_EXCLUSIVE_WRITE_GRANTED != 0 {
		s.flags &^= (SYNC_EXCLUSIVE_WRITE_GRANTED | SYNC_LOCK_READ_GRANTED)
		s.flags |= SYNC_EXCLUSIVE_WRITE
	} else if s.flags&SYNC_LOCK_READ_GRANTED != 0 {
		s.flags &^= SYNC_LOCK_READ_GRANTED
		s.flags |= SYNC_LOCK_READ
	}
	s.cond.Broadcast()
}

func (s *syncer) emptyLocked() bool {
	return s.readers == 0 && !s.writerActive
}
