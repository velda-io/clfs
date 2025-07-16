package vfs

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type mockRevokeNotifier struct {
	onRevokedCalled bool
	lastFlag        int
}

func (m *mockRevokeNotifier) OnRevoked(lastFlag int) {
	m.onRevokedCalled = true
	m.lastFlag = lastFlag
}

func TestSyncerSyncMode(t *testing.T) {
	// Server will handle the locks.
	t.Run("SyncModeNoBlocks", func(t *testing.T) {
		s := NewSyncer()

		op1 := s.StartWrite()
		assert.NotNil(t, op1)
		assert.False(t, op1.Async())

		op2 := s.StartWrite()
		assert.NotNil(t, op2)
		assert.False(t, op2.Async())

		op3 := s.StartRead()
		assert.NotNil(t, op3)
		assert.False(t, op3.Async())

		s.Complete(op1)
		s.Complete(op2)
		s.Complete(op3)
	})
}

// TestSyncerStartWrite tests starting a write operation
func TestSyncerStartWrite(t *testing.T) {
	t.Run("SimpleAsyncWrite", func(t *testing.T) {
		s := NewSyncer()
		s.UpgradeClaim(SYNC_EXCLUSIVE_WRITE, SYNC_EXCLUSIVE_WRITE)

		op := s.StartWrite()
		assert.NotNil(t, op)
		assert.True(t, op.Async())

		s.Complete(op)
	})

	// In async-network mode, all write ops should be in sequence
	t.Run("ConcurrentWritesAsync", func(t *testing.T) {
		s := NewSyncer()
		s.UpgradeClaim(SYNC_EXCLUSIVE_WRITE, SYNC_EXCLUSIVE_WRITE)

		var wg sync.WaitGroup
		wg.Add(2)

		var op1Completed, op2Completed bool
		var op1Started, op2Started bool
		go func() {
			defer wg.Done()
			op := s.StartWrite()
			op1Started = true
			assert.True(t, !op2Started || op2Completed, "Two writes cannot run concurrently")
			time.Sleep(50 * time.Millisecond)
			s.Complete(op)
			op1Completed = true
		}()

		go func() {
			defer wg.Done()
			// Wait to ensure the first goroutine has started
			time.Sleep(10 * time.Millisecond)

			op := s.StartWrite()
			op2Started = true

			assert.True(t, !op1Started || op1Completed, "Two writes cannot run concurrently")
			s.Complete(op)
			op2Completed = true
		}()

		wg.Wait()
		assert.False(t, s.writerActive) // No writers after completion
	})

	t.Run("WriteDuringRead", func(t *testing.T) {
		s := NewSyncer()
		s.UpgradeClaim(SYNC_EXCLUSIVE_WRITE, SYNC_EXCLUSIVE_WRITE)
		assert.Equal(t, s.flags, SYNC_EXCLUSIVE_WRITE)

		// Start a read operation
		readOp := s.StartRead()
		assert.Equal(t, 1, s.readers)

		// Start a write in a goroutine
		var writeStarted, writeCompleted bool
		var wg sync.WaitGroup
		wg.Add(1)

		go func() {
			defer wg.Done()
			writeStarted = true
			writeOp := s.StartWrite()
			// Write should only proceed after read is complete
			assert.Equal(t, 0, s.readers)
			s.Complete(writeOp)
			writeCompleted = true
		}()

		// Give the write goroutine a chance to try to start
		time.Sleep(50 * time.Millisecond)
		assert.True(t, writeStarted)
		assert.False(t, writeCompleted) // Write should be blocked

		// Complete the read operation
		s.Complete(readOp)

		// Wait for write to complete
		wg.Wait()
		assert.True(t, writeCompleted)
		assert.False(t, s.writerActive)
	})
}

// TestSyncerStartRead tests starting a read operation
func TestSyncerStartRead(t *testing.T) {
	t.Run("SimpleRead", func(t *testing.T) {
		s := NewSyncer()

		op := s.StartRead()
		assert.NotNil(t, op)
		assert.Equal(t, 1, s.readers)

		// Clean up
		s.Complete(op)
	})

	t.Run("MultipleReads", func(t *testing.T) {
		s := NewSyncer()

		// Start multiple read operations concurrently
		var wg sync.WaitGroup
		const numReaders = 5
		wg.Add(numReaders)

		ops := make([]*operation, numReaders)

		for i := 0; i < numReaders; i++ {
			go func(idx int) {
				defer wg.Done()
				ops[idx] = s.StartRead()
			}(i)
		}

		wg.Wait()

		assert.Equal(t, numReaders, s.readers)

		// Clean up
		for _, op := range ops {
			s.Complete(op)
		}

		assert.Equal(t, 0, s.readers)
	})

	t.Run("ReadDuringWrite", func(t *testing.T) {
		s := NewSyncer()
		s.UpgradeClaim(SYNC_EXCLUSIVE_WRITE, SYNC_EXCLUSIVE_WRITE)

		// Start a write operation
		writeOp := s.StartWrite()
		assert.True(t, s.writerActive)

		// Try to start a read in a goroutine
		var readStarted, readCompleted bool
		var wg sync.WaitGroup
		wg.Add(1)

		go func() {
			defer wg.Done()
			readStarted = true
			readOp := s.StartRead()
			// Read should only proceed after write is complete
			assert.False(t, s.writerActive)
			s.Complete(readOp)
			readCompleted = true
		}()

		// Give the read goroutine a chance to try to start
		time.Sleep(50 * time.Millisecond)
		assert.True(t, readStarted)
		assert.False(t, readCompleted) // Read should be blocked

		// Complete the write operation
		s.Complete(writeOp)

		// Wait for read to complete
		wg.Wait()
		assert.True(t, readCompleted)
		assert.Equal(t, 0, s.readers)
	})
}

// TestSyncerUpgradeClaim tests the claim upgrade functionality
func TestSyncerUpgradeClaim(t *testing.T) {
	t.Run("UpgradeToExclusiveWrite", func(t *testing.T) {
		s := NewSyncer()

		op1 := s.StartRead()
		op2 := s.StartWrite()
		assert.False(t, op1.Async(), "Read operation should not be async before upgrade")
		assert.False(t, op2.Async(), "Write operation should not be async before upgrade")
		wg := sync.WaitGroup{}
		wg.Add(2)
		s.UpgradeClaim(SYNC_EXCLUSIVE_WRITE, SYNC_EXCLUSIVE_WRITE)
		var op3, op4 *operation
		go func() {
			op3 = s.StartWrite()
			defer s.Complete(op3)
			wg.Done()
		}()

		go func() {
			op4 = s.StartRead()
			defer s.Complete(op4)
			wg.Done()
		}()
		time.Sleep(100 * time.Millisecond) // Verify being blocked
		assert.Nil(t, op3, "Write operation should not start while upgrade is pending")
		assert.Nil(t, op4, "Read operation should not start while upgrade is pending")

		s.Complete(op1)
		s.Complete(op2)

		wg.Wait()
		assert.True(t, op3.Async(), "Write operation should be async after upgrade")
		assert.True(t, op4.Async(), "Read operation should be async after upgrade")
	})

	t.Run("UpgradeToLockRead", func(t *testing.T) {
		s := NewSyncer()

		op1 := s.StartRead()
		op2 := s.StartWrite()
		assert.False(t, op1.Async(), "Read operation should not be async before upgrade")
		assert.False(t, op2.Async(), "Write operation should not be async before upgrade")
		wg := sync.WaitGroup{}
		wg.Add(1)
		s.UpgradeClaim(SYNC_LOCK_READ, SYNC_LOCK_READ)
		var op3 *operation

		go func() {
			op3 = s.StartRead()
			defer s.Complete(op3)
			wg.Done()
		}()
		time.Sleep(100 * time.Millisecond) // Verify being blocked
		assert.Nil(t, op3, "Read operation should not start while upgrade is pending")

		s.Complete(op1)
		s.Complete(op2)

		wg.Wait()
		assert.True(t, op3.Async(), "Read operation should be async after upgrade")
	})

	t.Run("WriteCancelReadUpgrade", func(t *testing.T) {
		s := NewSyncer()

		op1 := s.StartRead()
		op2 := s.StartWrite()
		assert.False(t, op1.Async(), "Read operation should not be async before upgrade")
		assert.False(t, op2.Async(), "Write operation should not be async before upgrade")
		wg := sync.WaitGroup{}
		wg.Add(1)
		s.UpgradeClaim(SYNC_LOCK_READ, SYNC_LOCK_READ)
		var op3 *operation

		go func() {
			op3 = s.StartRead()
			defer s.Complete(op3)
			wg.Done()
		}()
		time.Sleep(100 * time.Millisecond) // op3 started but blocked
		assert.Nil(t, op3, "Read operation should not start while upgrade is pending")
		op4 := s.StartWrite()
		// The upgrade should be cancelled now. Op3 should start as sync mode.
		assert.False(t, op4.Async(), "Write operation should not be async with only read lock.")

		s.Complete(op1)
		s.Complete(op2)

		s.Complete(op4)

		wg.Wait()
		assert.Equal(t, s.flags, 0, "Flags should be reset after write")
		assert.False(t, op3.Async(), "Read operation should not be async after write triggered cancel")
	})
}

// TestSyncerUpgradeClaim tests the claim upgrade functionality
func TestSyncerRevokeClaim(t *testing.T) {
	t.Run("RevokeExclusiveWrite", func(t *testing.T) {
		s := NewSyncer()
		notifier := &mockRevokeNotifier{}
		s.SetOnRevoked(notifier)

		s.UpgradeClaim(SYNC_EXCLUSIVE_WRITE, SYNC_EXCLUSIVE_WRITE)
		op1 := s.StartWrite()
		assert.True(t, op1.Async(), "Write operation should be async before upgrade")
		wg := sync.WaitGroup{}
		wg.Add(2)
		s.UpgradeClaim(SYNC_EXCLUSIVE_WRITE, 0)
		var op2, op3 *operation
		go func() {
			op2 = s.StartWrite()
			defer s.Complete(op2)
			wg.Done()
		}()

		go func() {
			op3 = s.StartRead()
			defer s.Complete(op3)
			wg.Done()
		}()

		s.Complete(op1)
		time.Sleep(100 * time.Millisecond) // Verify being blocked
		assert.Nil(t, op2, "Write operation should not start while upgrade is pending")
		assert.Nil(t, op3, "Read operation should not start while upgrade is pending")
		assert.False(t, notifier.onRevokedCalled, "OnRevoked should not be called yet")
		s.CompleteAsync(op1)

		wg.Wait()
		assert.True(t, notifier.onRevokedCalled, "OnRevoked should be called after upgrade")
		assert.False(t, op2.Async(), "Write operation should not be async after revocation")
		assert.False(t, op3.Async(), "Read operation should not be async after revocation")
	})

	t.Run("RevokeLockRead", func(t *testing.T) {
		s := NewSyncer()
		notifier := &mockRevokeNotifier{}
		s.SetOnRevoked(notifier)

		s.UpgradeClaim(SYNC_LOCK_READ, SYNC_LOCK_READ)
		op1 := s.StartRead()
		assert.True(t, op1.Async(), "Read operation should be async before revocation")
		wg := sync.WaitGroup{}
		wg.Add(1)
		s.UpgradeClaim(SYNC_LOCK_READ, 0)
		var op2 *operation

		// TODO: This could be made not blocked until revocation is complete
		go func() {
			op2 = s.StartRead()
			defer s.Complete(op2)
			wg.Done()
		}()
		time.Sleep(100 * time.Millisecond) // Verify being blocked
		assert.Nil(t, op2, "Read operation should not start while upgrade is pending")

		assert.False(t, notifier.onRevokedCalled, "OnRevoked should not be called yet")
		s.Complete(op1)

		wg.Wait()
		assert.True(t, notifier.onRevokedCalled, "OnRevoked should be called after revoke")
		assert.False(t, op2.Async(), "Read operation should not be async after revocation")
	})
}
