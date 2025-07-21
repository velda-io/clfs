package server

import "sync"

// L: Node
// R: Session
type claimTracker struct {
	mu      sync.Mutex
	writer  *session
	readers map[*session]bool
	queue   []func()
	updater claimUpdater
}

type claimUpdater interface {
	NotifyRevokeWriter(s *session)
	NotifyRevokeReader(s *session)
	ClaimWriter(s *session) bool
	ClaimReader(s *session) bool
}

func NewClaimTracker(updater claimUpdater) *claimTracker {
	return &claimTracker{
		readers: make(map[*session]bool),
		updater: updater,
	}
}

// Directly set the writer to the session without any checks.
// This should only be used when the session is already known to be the writer.
func (t *claimTracker) setWriter(s *session) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.writer != nil || len(t.readers) > 0 {
		panic("Cannot set writer when there are active readers or another writer")
	}
	t.writer = s
	s.AddWriterClaim(t)
}

func (t *claimTracker) Write(s *session, callback func()) {
	t.mu.Lock()
	defer t.mu.Unlock()
	noQueue := len(t.queue) == 0
	// Currently holds the writer claim and not revoking
	if t.writer == s {
		callback()
		return
	}
	if t.writer != nil && noQueue {
		debugf("%v: Revoking writer %v, write from %v", t, t.writer, s)
		t.updater.NotifyRevokeWriter(t.writer)
	}
	if len(t.readers) > 0 && noQueue {
		for reader := range t.readers {
			t.updater.NotifyRevokeReader(reader)
		}
	}
	if t.writer == nil && len(t.readers) == 0 {
		if t.updater.ClaimWriter(s) {
			t.writer = s
			s.AddWriterClaim(t)
		}
		callback()
		return
	}
	t.queue = append(t.queue, callback)
}

func (t *claimTracker) Read(s *session, callback func()) {
	t.mu.Lock()
	defer t.mu.Unlock()

	noQueue := len(t.queue) == 0
	// Writer is claimed.
	if t.writer != nil {
		if noQueue {
			if t.writer == s {
				callback()
				return
			}
			debugf("%v: Revoking writer %v, read from %v", t, t.writer, s)
			t.updater.NotifyRevokeWriter(t.writer)
		}
		t.queue = append(t.queue, callback)
		return
	}

	if len(t.queue) != 0 {
		// Avoid starvation by allow write to proceed
		// E.g. readers are being revoked.
		t.queue = append(t.queue, callback)
		return
	}

	// Proceed to read.
	if _, exists := t.readers[s]; !exists {
		if t.updater.ClaimReader(s) {
			t.readers[s] = true
			s.AddReaderClaim(t)
		}
	}
	callback()
}

// Caller needs to remove it from session's live claims.
func (t *claimTracker) RevokedWriter(s *session) {
	t.mu.Lock()
	defer t.mu.Unlock()
	debugf("%v: Writer %v revoked", t, s)
	if t.writer == s {
		t.writer = nil
	}
	t.checkQueues()
}

// Caller needs to remove it from session's live claims.
func (t *claimTracker) RevokedReader(s *session) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, exists := t.readers[s]; exists {
		delete(t.readers, s)
		debugf("%v: Reader %v revoked", t, s)
		t.checkQueues()
	}
}

func (t *claimTracker) checkQueues() {
	if t.writer == nil && len(t.readers) == 0 {
		debugf("%v: No active claims, processing queue", t)
		for _, callback := range t.queue {
			callback()
		}
		t.queue = nil
	}
}
