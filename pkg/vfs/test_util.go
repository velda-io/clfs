package vfs

import (
	"sync"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/mock"
	"velda.io/mtfs/pkg/proto"
)

// MockServerProtocol is a mock implementation of ServerProtocol for testing
type MockServerProtocol struct {
	mock.Mock
	callbacks map[string]ServerCallback
	mu        sync.Mutex
	t         *testing.T
}

func NewMockServerProtocol(t *testing.T) *MockServerProtocol {
	return &MockServerProtocol{
		callbacks: make(map[string]ServerCallback),
		t:         t,
	}
}

func (m *MockServerProtocol) EnqueueOperation(request *proto.OperationRequest, callback OpCallback) int64 {
	args := m.Called(request, callback)
	val := args.Get(0)
	if i, ok := val.(int64); ok {
		return i
	}
	return int64(args.Int(0))
}

func (m *MockServerProtocol) RegisterServerCallback(cookie []byte, callback ServerCallback) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Called(cookie, mock.Anything)
	m.callbacks[string(cookie)] = callback
}

func (m *MockServerProtocol) UnregisterServerCallback(cookie []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Called(cookie)
	delete(m.callbacks, string(cookie))
}

func (m *MockServerProtocol) ReportAsyncError(fmt string, args ...interface{}) {
	m.t.Errorf(fmt, args...)
}

// Helper to trigger a response callback
func (m *MockServerProtocol) TriggerCallback(cookie []byte, response *proto.OperationResponse) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if cb, ok := m.callbacks[string(cookie)]; ok {
		cb(response)
	}
}

type DummyServer struct {
	t *testing.T
}

func (d *DummyServer) EnqueueOperation(request *proto.OperationRequest, callback OpCallback) int64 {
	go callback(nil, syscall.EIO)
	return 1
}

func (d *DummyServer) RegisterServerCallback(cookie []byte, callback ServerCallback) {
}

func (d *DummyServer) UnregisterServerCallback(cookie []byte) {
}

func (d *DummyServer) ReportAsyncError(fmt string, args ...interface{}) {
	d.t.Errorf(fmt, args...)
}

func testMount(t *testing.T, root fs.InodeEmbedder, opts *fs.Options) (string, *fuse.Server) {
	t.Helper()

	mntDir := t.TempDir()
	if opts == nil {
		opts = &fs.Options{
			FirstAutomaticIno: 1,
		}
	}

	server, err := fs.Mount(mntDir, root, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := server.Unmount(); err != nil {
			t.Fatalf("testMount: Unmount failed: %v", err)
		}
	})
	return mntDir, server
}
