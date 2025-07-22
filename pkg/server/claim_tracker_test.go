package server

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testClaimUpdater is a test implementation of claimUpdater
type testClaimUpdater struct {
	mu             sync.Mutex
	revokeWriterCh chan *session
	revokeReaderCh chan *session
	revokeDentryCh chan struct {
		session *session
		dentry  string
	}
	claimWriterRes   bool
	claimReaderRes   bool
	revokedWriters   []*session
	revokedReaders   []*session
	revokedDentries  map[*session][]string
	writersClaimable map[*session]bool
	readersClaimable map[*session]bool
}

func newTestClaimUpdater() *testClaimUpdater {
	return &testClaimUpdater{
		revokeWriterCh: make(chan *session, 10),
		revokeReaderCh: make(chan *session, 10),
		revokeDentryCh: make(chan struct {
			session *session
			dentry  string
		}, 10),
		revokedDentries:  make(map[*session][]string),
		writersClaimable: make(map[*session]bool),
		readersClaimable: make(map[*session]bool),
	}
}

func (t *testClaimUpdater) NotifyRevokeWriter(s *session) {
	t.mu.Lock()
	t.revokedWriters = append(t.revokedWriters, s)
	t.mu.Unlock()
	t.revokeWriterCh <- s
}

func (t *testClaimUpdater) NotifyRevokeReader(s *session) {
	t.mu.Lock()
	t.revokedReaders = append(t.revokedReaders, s)
	t.mu.Unlock()
	t.revokeReaderCh <- s
}

func (t *testClaimUpdater) NotifyRevokeDentry(s *session, dentry string) {
	t.mu.Lock()
	if t.revokedDentries == nil {
		t.revokedDentries = make(map[*session][]string)
	}
	t.revokedDentries[s] = append(t.revokedDentries[s], dentry)
	t.mu.Unlock()
	t.revokeDentryCh <- struct {
		session *session
		dentry  string
	}{session: s, dentry: dentry}
}

func (t *testClaimUpdater) ClaimWriter(s *session) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.writersClaimable[s]
}

func (t *testClaimUpdater) ClaimReader(s *session) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.readersClaimable[s]
}

// TestWriteBlockedByWriter tests that write operations are blocked when another session
// holds a writer claim, and that the claim is revoked.
func TestClaimWriteBlockedByWriter(t *testing.T) {
	// Setup
	updater := newTestClaimUpdater()
	tracker := NewClaimTracker(updater)

	// Create two sessions
	session1 := createMockSession(t)
	session2 := createMockSession(t)

	// Make session1 claimable
	updater.writersClaimable[session1] = true
	updater.writersClaimable[session2] = false

	// Session1 claims writer
	writeDone := false
	tracker.Write(session1, "", func() {
		writeDone = true
	})
	require.True(t, writeDone, "Write operation should succeed for the first session")
	assert.Equal(t, session1, tracker.writer, "Session1 should be the writer")

	// Try to write with session2, which should be blocked
	write2Done := false
	write2CallbackCh := make(chan struct{})
	go func() {
		tracker.Write(session2, "", func() {
			write2Done = true
			write2CallbackCh <- struct{}{}
		})
	}()

	// Verify revoke was requested
	select {
	case revokedSession := <-updater.revokeWriterCh:
		assert.Equal(t, session1, revokedSession, "Wrong session received revoke notification")
	case <-time.After(time.Second):
		assert.Fail(t, "Timeout waiting for revoke notification")
	}

	// Verify write2 is still blocked
	select {
	case <-write2CallbackCh:
		assert.Fail(t, "Write2 callback should not have been called yet")
	case <-time.After(10 * time.Millisecond):
		// Expected - write2 is still blocked
	}

	// session1 can still write until the revocation is confirmed
	write3Done := false
	tracker.Write(session1, "", func() {
		write3Done = true
	})
	require.True(t, write3Done, "Write operation should succeed for the first session")
	assert.Equal(t, session1, tracker.writer, "Session1 should be the writer")

	// Now revoke the writer claim from session1
	go tracker.RevokedWriter(session1)

	// Verify write2 now succeeds
	select {
	case <-write2CallbackCh:
		assert.True(t, write2Done, "Write2 callback should have set write2Done")
		assert.Nil(t, tracker.writer, "Writer should be nil after revocation")
	case <-time.After(time.Second):
		assert.Fail(t, "Timeout waiting for write2 callback")
	}
}

// TestReadBlockedByWriter tests that read operations are blocked when another session
// holds a writer claim, and that the writer claim is revoked.
func TestClaimReadBlockedByWriter(t *testing.T) {
	// Setup
	updater := newTestClaimUpdater()
	tracker := NewClaimTracker(updater)

	// Create two sessions
	writer := createMockSession(t)
	reader := createMockSession(t)

	// Make sessions claimable
	updater.writersClaimable[writer] = true
	updater.readersClaimable[reader] = false

	// Writer session claims writer
	writeDone := false
	tracker.Write(writer, "", func() {
		writeDone = true
	})
	require.True(t, writeDone, "Write operation should succeed for the writer session")
	assert.Equal(t, writer, tracker.writer, "Writer session should be the writer")

	// Try to read with reader session, which should be blocked
	readDone := false
	readCallbackCh := make(chan struct{})
	go func() {
		tracker.Read(reader, func() {
			readDone = true
			readCallbackCh <- struct{}{}
		})
	}()

	// Verify revoke was requested
	select {
	case revokedSession := <-updater.revokeWriterCh:
		assert.Equal(t, writer, revokedSession, "Wrong session received revoke notification")
	case <-time.After(time.Second):
		assert.Fail(t, "Timeout waiting for revoke notification")
	}

	// Verify read is still blocked
	select {
	case <-readCallbackCh:
		assert.Fail(t, "Read callback should not have been called yet")
	case <-time.After(10 * time.Millisecond):
		// Expected - read is still blocked
	}

	// Now revoke the writer claim
	go tracker.RevokedWriter(writer)

	// Verify read now succeeds
	select {
	case <-readCallbackCh:
		assert.True(t, readDone, "Read callback should have set readDone")
		assert.Len(t, tracker.readers, 0, "Should have one reader")
	case <-time.After(time.Second):
		assert.Fail(t, "Timeout waiting for read callback")
	}
}

// TestWriteBlockedByReaders tests that write operations are blocked when other sessions
// hold reader claims, and that the reader claims are revoked.
func TestClaimWriteBlockedByReaders(t *testing.T) {
	// Setup
	updater := newTestClaimUpdater()
	tracker := NewClaimTracker(updater)

	// Create three sessions
	reader1 := createMockSession(t)
	reader2 := createMockSession(t)
	writer := createMockSession(t)

	// Make sessions claimable
	updater.readersClaimable[reader1] = true
	updater.readersClaimable[reader2] = true
	updater.writersClaimable[writer] = false

	// Reader sessions claim readers
	read1Done := false
	tracker.Read(reader1, func() {
		read1Done = true
	})
	require.True(t, read1Done, "Read operation should succeed for reader1")

	read2Done := false
	tracker.Read(reader2, func() {
		read2Done = true
	})
	require.True(t, read2Done, "Read operation should succeed for reader2")

	assert.Len(t, tracker.readers, 2, "Should have two reader sessions")

	// Try to write, which should be blocked
	writeDone := false
	writeCallbackCh := make(chan struct{})
	go func() {
		tracker.Write(writer, "", func() {
			writeDone = true
			writeCallbackCh <- struct{}{}
		})
	}()

	// Collect all revoke notifications (should be two)
	revokedReaderCount := 0
	revokedReaders := make(map[*session]bool)
	for i := 0; i < 2; i++ {
		select {
		case revokedSession := <-updater.revokeReaderCh:
			revokedReaderCount++
			revokedReaders[revokedSession] = true
		case <-time.After(time.Second):
			assert.Failf(t, "Timeout waiting for reader revoke notification", "notification #%d", i+1)
		}
	}

	assert.Equal(t, 2, revokedReaderCount, "Expected 2 reader revoke notifications")
	assert.True(t, revokedReaders[reader1], "reader1 should have received revoke notification")
	assert.True(t, revokedReaders[reader2], "reader2 should have received revoke notification")

	// Verify write is still blocked
	select {
	case <-writeCallbackCh:
		assert.Fail(t, "Write callback should not have been called yet")
	case <-time.After(10 * time.Millisecond):
		// Expected - write is still blocked
	}

	// Now revoke both reader claims
	go tracker.RevokedReader(reader1)

	// Verify write is still blocked with one reader remaining
	select {
	case <-writeCallbackCh:
		assert.Fail(t, "Write callback should not have been called yet")
	case <-time.After(10 * time.Millisecond):
		// Expected - write is still blocked with one reader remaining
	}

	go tracker.RevokedReader(reader2)

	// Verify write now succeeds
	select {
	case <-writeCallbackCh:
		assert.True(t, writeDone, "Write callback should have set writeDone")
		assert.Nil(t, tracker.writer, "Writer should now be nil")
	case <-time.After(time.Second):
		assert.Fail(t, "Timeout waiting for write callback")
	}
}

// TestConcurrentClaimsAndRevocations tests how the claim tracker handles concurrent
// operations, ensuring proper queuing and execution order
func TestClaimConcurrentClaimsAndRevocations(t *testing.T) {
	// Setup
	updater := newTestClaimUpdater()
	tracker := NewClaimTracker(updater)

	// Create sessions for different operations
	writer1 := createMockSession(t)
	writer2 := createMockSession(t)
	reader1 := createMockSession(t)
	reader2 := createMockSession(t)

	// Make sessions claimable
	updater.writersClaimable[writer1] = true
	updater.writersClaimable[writer2] = false
	updater.readersClaimable[reader1] = false
	updater.readersClaimable[reader2] = false

	// Start with writer1 having the writer claim
	writer1Done := false
	tracker.Write(writer1, "", func() {
		writer1Done = true
	})
	require.True(t, writer1Done, "Write operation should succeed for the first writer")
	assert.Equal(t, writer1, tracker.writer, "Writer1 should be the current writer")

	// Queue up multiple operations
	var wg sync.WaitGroup
	wg.Add(4) // 1 revoke, 1 writer and 2 readers

	go func() {
		revokedSession := <-updater.revokeWriterCh
		assert.Equal(t, writer1, revokedSession, "Wrong session received revoke notification")
		tracker.RevokedWriter(writer1)
		wg.Done()
	}()
	// Queue writer2
	tracker.Write(writer2, "", func() {
		defer wg.Done()
	})

	// Queue reader1 and reader2
	tracker.Read(reader1, func() {
		defer wg.Done()
	})

	tracker.Read(reader2, func() {
		defer wg.Done()
	})

	wg.Wait()
	assert.Nil(t, tracker.writer, "Writer should be nil after revocation")
}

func TestDentryClaimsAndRevocations(t *testing.T) {
	// Setup
	updater := newTestClaimUpdater()
	tracker := NewClaimTracker(updater)
	tracker.dentries = make(map[string]*dentryClaimTracker)

	// Create sessions for different operations
	session1 := createMockSession(t)
	session2 := createMockSession(t)

	dentry1 := "dir1"
	claimDone := false
	tracker.Read(session1, func() {
		tracker.AddDentryClaim(session1, dentry1)
		claimDone = true
	})

	// Session1 writes with dentry "dir1"
	require.True(t, claimDone, "Read operation should not block and should claim dentry")

	// Try to write with session2 to the same dentry, which should be blocked
	write2CallbackCh := make(chan struct{})
	go func() {
		tracker.Write(session2, dentry1, func() {
			close(write2CallbackCh)
		})
	}()

	// Verify dentry revoke was requested
	select {
	case revoke := <-updater.revokeDentryCh:
		assert.Equal(t, session1, revoke.session, "Wrong session received revoke notification")
		assert.Equal(t, dentry1, revoke.dentry, "Wrong dentry received in revoke notification")
	case <-time.After(time.Second):
		assert.Fail(t, "Timeout waiting for dentry revoke notification")
	}

	// Verify write2 is still blocked
	select {
	case <-write2CallbackCh:
		assert.Fail(t, "Write2 callback should not have been called yet")
	case <-time.After(10 * time.Millisecond):
		// Expected - write2 is still blocked
	}

	// Now revoke the dentry claim
	tracker.RevokedDentry(session1, dentry1)

	// Verify write2 now succeeds
	select {
	case <-write2CallbackCh:
	case <-time.After(time.Second):
		assert.Fail(t, "Timeout waiting for write2 callback")
	}
}
