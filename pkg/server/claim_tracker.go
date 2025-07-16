package server

import "sync"

// L: Node
// R: Session
type claimTracker struct {
	mu         sync.Mutex
	writer     *session
	readers    map[*session]bool
	writeQueue []func()
	readQueue  []func()
	updater    claimUpdater
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

func (t *claimTracker) Write(s *session, callback func()) {
	t.mu.Lock()
	defer t.mu.Unlock()
	noQueue := len(t.writeQueue) == 0 && len(t.readQueue) == 0
	// Currently holds the writer claim and not revoking
	if t.writer == s {
		callback()
		return
	}
	if t.writer != nil && noQueue {
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
	t.writeQueue = append(t.writeQueue, callback)
}

func (t *claimTracker) Read(s *session, callback func()) {
	t.mu.Lock()
	defer t.mu.Unlock()

	noQueue := len(t.writeQueue) == 0 && len(t.readQueue) == 0
	// Writer is claimed.
	if t.writer != nil {
		if noQueue {
			if t.writer == s {
				callback()
				return
			}
			t.updater.NotifyRevokeWriter(t.writer)
		}
		t.readQueue = append(t.readQueue, callback)
		return
	}

	if len(t.writeQueue) != 0 {
		// Avoid starvation by allow write to proceed
		// E.g. readers are being revoked.
		t.readQueue = append(t.readQueue, callback)
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
		t.checkQueues()
	}
}

func (t *claimTracker) checkQueues() {
	if t.writer == nil && len(t.readers) == 0 {
		for _, callback := range t.writeQueue {
			callback()
		}
		t.writeQueue = nil
		for _, callback := range t.readQueue {
			callback()
		}
		t.readQueue = nil
	}
}
