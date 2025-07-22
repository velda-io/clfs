package server

import (
	"sync"
)

type dentryClaimTracker struct {
	sessions map[*session]bool
	queue    []func()
}
type claimTracker struct {
	mu       sync.Mutex
	writer   *session
	readers  map[*session]bool
	queue    []func()
	updater  claimUpdater
	dentries map[string]*dentryClaimTracker
}

type claimUpdater interface {
	NotifyRevokeWriter(s *session)
	NotifyRevokeReader(s *session)
	NotifyRevokeDentry(s *session, dentry string)
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
	debugf("%p: Setting writer to %p", t, s)
	t.writer = s
	s.AddWriterClaim(t)
}

func (t *claimTracker) AddDentryClaim(s *session, dentry string) {
	if t.writer != nil || len(t.readers) > 0 {
		// No per-dentry tracking.
		return
	}
	// Must be locked by the caller
	if t.dentries == nil {
		t.dentries = make(map[string]*dentryClaimTracker)
	}
	if d, exists := t.dentries[dentry]; !exists {
		t.dentries[dentry] = &dentryClaimTracker{
			sessions: map[*session]bool{s: false},
			queue:    nil,
		}
	} else {
		d.sessions[s] = false // Not revoked yet
	}
}

func (t *claimTracker) Write(s *session, dentry string, callback func()) {
	t.mu.Lock()
	defer t.mu.Unlock()
	noQueue := len(t.queue) == 0
	// Currently holds the writer claim and not revoking
	if t.writer == s {
		callback()
		return
	}
	if t.writer != nil && noQueue {
		debugf("%p: Revoking writer %p, write from %p", t, t.writer, s)
		t.updater.NotifyRevokeWriter(t.writer)
	}
	if len(t.readers) > 0 && noQueue {
		for reader := range t.readers {
			t.updater.NotifyRevokeReader(reader)
		}
	}
	if t.writer == nil && len(t.readers) == 0 {
		if t.updater.ClaimWriter(s) {
			debugf("%p: Writer %p claimed", t, s)
			t.writer = s
			s.AddWriterClaim(t)
			t.dentries = nil
			callback()
			return
		}
		dentryClaimed := true
		if dentry != "" {
			dentryClaims := t.dentries[dentry]
			if dentryClaims != nil {
				delete(dentryClaims.sessions, s)
				for ses, revoked := range dentryClaims.sessions {
					if !revoked && ses != s {
						t.updater.NotifyRevokeDentry(ses, dentry)
						dentryClaims.sessions[ses] = true // Mark as revoked
						dentryClaimed = false
					} else if ses != s {
						dentryClaimed = false
					}
				}
				if !dentryClaimed {
					dentryClaims.queue = append(dentryClaims.queue, callback)
					return
				}
			}
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
			debugf("%p: Revoking writer %p, read from %p", t, t.writer, s)
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
			t.dentries = nil
		}
	}
	callback()
}

// Caller needs to remove it from session's live claims.
func (t *claimTracker) RevokedWriter(s *session) {
	t.mu.Lock()
	defer t.mu.Unlock()
	debugf("%p: Writer %p revoked", t, s)
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
		debugf("%p: Reader %p revoked", t, s)
		t.checkQueues()
	}
}

func (t *claimTracker) RevokedDentry(s *session, dentry string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if dentryClaims, exists := t.dentries[dentry]; exists {
		if _, exists := dentryClaims.sessions[s]; exists {
			delete(dentryClaims.sessions, s)
			debugf("%p: Dentry %s revoked for %p", t, dentry, s)
		}
		if len(dentryClaims.sessions) == 0 {
			queue := dentryClaims.queue
			delete(t.dentries, dentry)
			for _, callback := range queue {
				callback()
			}
		}
	} else {
		debugf("%p: Dentry %s not found for %p", t, dentry, s)
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
