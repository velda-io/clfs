package vfs

import (
	"context"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"velda.io/mtfs/pkg/proto"
)

// MockServerProtocol is a mock implementation of ServerProtocol for testing
type MockServerProtocol struct {
	mock.Mock
	callbacks map[string]ServerCallback
	mu        sync.Mutex
}

func NewMockServerProtocol() *MockServerProtocol {
	return &MockServerProtocol{
		callbacks: make(map[string]ServerCallback),
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

// Helper to trigger a response callback
func (m *MockServerProtocol) TriggerCallback(cookie []byte, response *proto.OperationResponse) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if cb, ok := m.callbacks[string(cookie)]; ok {
		cb(response)
	}
}

// TestMkdirAsyncRequest verifies that in async mode the Mkdir method sends the correct request
func TestMkdirAsyncRequest(t *testing.T) {
	// Setup
	mockServer := NewMockServerProtocol()
	cookie := []byte("parent-dir-cookie")

	// Mock will track but not enforce the RegisterServerCallback call
	mockServer.On("RegisterServerCallback", mock.Anything, mock.Anything).Return()

	// Create parent inode with SYNC_EXCLUSIVE_WRITE flag (async mode)
	inode := NewInode(mockServer, cookie, SYNC_EXCLUSIVE_WRITE)

	// Define new directory cookie
	newDirCookie := []byte("new-dir-cookie")

	// Set up the mock server's EnqueueOperation expectation and capture the request
	var capturedRequest *proto.OperationRequest
	mockServer.On("EnqueueOperation", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		// Capture the request for validation
		capturedRequest = args.Get(0).(*proto.OperationRequest)

		// Get the callback and call it with a success response
		callback := args.Get(1).(OpCallback)
		callback(&proto.OperationResponse{
			Response: &proto.OperationResponse_Mkdir{
				Mkdir: &proto.MkdirResponse{
					Cookie: newDirCookie,
					Stat: &proto.FileStat{
						Mode: syscall.S_IFDIR | 0755,
					},
				},
			},
		}, nil)
	}).Return(int64(1))

	// No need to create operation objects directly in our tests

	// Call the asyncOperation directly to test request handling
	ctx := context.Background()
	inode.asyncOperation(ctx, &proto.OperationRequest{
		Operation: &proto.OperationRequest_Mkdir{
			Mkdir: &proto.MkdirRequest{
				Name: "testdir",
				Stat: emptyFileStatProto(ctx, syscall.S_IFDIR|0755),
			},
		},
	}, func(response *proto.OperationResponse, err error) {
		// This callback will be called by the mock above
		// Verify the response is correct
		assert.Nil(t, err)
		assert.NotNil(t, response)

		mkdirResp, ok := response.Response.(*proto.OperationResponse_Mkdir)
		assert.True(t, ok)
		assert.Equal(t, newDirCookie, mkdirResp.Mkdir.Cookie)
	})

	// Allow some time for async callbacks to complete
	time.Sleep(10 * time.Millisecond)

	// Verify the request was sent with the correct parameters
	assert.NotNil(t, capturedRequest)
	assert.Equal(t, cookie, capturedRequest.Cookie)

	// Verify the mkdir operation details
	mkdirReq, ok := capturedRequest.Operation.(*proto.OperationRequest_Mkdir)
	assert.True(t, ok, "Expected Mkdir operation")
	assert.Equal(t, "testdir", mkdirReq.Mkdir.Name)
	assert.Equal(t, uint32(syscall.S_IFDIR|0755), mkdirReq.Mkdir.Stat.Mode)

	// Verify all mock expectations were met
	mockServer.AssertExpectations(t)
}

// TestMkdirSyncRequest verifies that in sync mode the Mkdir method sends the correct request
func TestMkdirSyncRequest(t *testing.T) {
	// Setup
	mockServer := NewMockServerProtocol()
	cookie := []byte("parent-dir-cookie")

	// Mock will track but not enforce the RegisterServerCallback call
	mockServer.On("RegisterServerCallback", mock.Anything, mock.Anything).Return()

	// Create parent inode with no sync flags (sync mode)
	inode := NewInode(mockServer, cookie, 0)

	// Define new directory cookie
	newDirCookie := []byte("new-dir-cookie")

	// Set up the mock server's EnqueueOperation expectation and capture the request
	var capturedRequest *proto.OperationRequest
	mockServer.On("EnqueueOperation", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		// Capture the request for validation
		capturedRequest = args.Get(0).(*proto.OperationRequest)

		// Get the callback and call it with a success response
		callback := args.Get(1).(OpCallback)
		callback(&proto.OperationResponse{
			Response: &proto.OperationResponse_Mkdir{
				Mkdir: &proto.MkdirResponse{
					Cookie: newDirCookie,
					Stat: &proto.FileStat{
						Mode: syscall.S_IFDIR | 0755,
					},
				},
			},
		}, nil)
	}).Return(int64(1))

	// Call syncOperation directly to test the sync request flow
	ctx := context.Background()
	response, err := inode.syncOperation(ctx, &proto.OperationRequest{
		Cookie: cookie,
		Operation: &proto.OperationRequest_Mkdir{
			Mkdir: &proto.MkdirRequest{
				Name: "testdir",
				Stat: emptyFileStatProto(ctx, syscall.S_IFDIR|0755),
			},
		},
	})

	// Verify the sync operation was successful
	assert.Nil(t, err)
	assert.NotNil(t, response)

	// Verify response type is correct
	mkdirResp, ok := response.Response.(*proto.OperationResponse_Mkdir)
	assert.True(t, ok)
	assert.Equal(t, newDirCookie, mkdirResp.Mkdir.Cookie)

	// Verify the request was sent with the correct parameters
	assert.NotNil(t, capturedRequest)
	assert.Equal(t, cookie, capturedRequest.Cookie)

	// Verify the mkdir operation details
	mkdirReq, ok := capturedRequest.Operation.(*proto.OperationRequest_Mkdir)
	assert.True(t, ok, "Expected Mkdir operation")
	assert.Equal(t, "testdir", mkdirReq.Mkdir.Name)
	assert.Equal(t, uint32(syscall.S_IFDIR|0755), mkdirReq.Mkdir.Stat.Mode)

	// Verify all mock expectations were met
	mockServer.AssertExpectations(t)
}

// TestMkdirWithPendingCookie tests the case where the parent node doesn't have a cookie yet
func TestMkdirWithPendingCookie(t *testing.T) {
	// Setup
	mockServer := NewMockServerProtocol()

	// Create parent inode with no cookie (nil)
	inode := NewInode(mockServer, nil, SYNC_EXCLUSIVE_WRITE)

	// Define cookies
	parentCookie := []byte("parent-dir-cookie")
	newDirCookie := []byte("new-dir-cookie")

	// Mock will track but not enforce the RegisterServerCallback call
	mockServer.On("RegisterServerCallback", mock.Anything, mock.Anything).Return()

	// Set up EnqueueOperation expectation for when the cookie is resolved
	var capturedRequest *proto.OperationRequest
	mockServer.On("EnqueueOperation", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		// Capture the request for validation
		capturedRequest = args.Get(0).(*proto.OperationRequest)

		// Get the callback and call it with a success response
		callback := args.Get(1).(OpCallback)
		callback(&proto.OperationResponse{
			Response: &proto.OperationResponse_Mkdir{
				Mkdir: &proto.MkdirResponse{
					Cookie: newDirCookie,
					Stat: &proto.FileStat{
						Mode: syscall.S_IFDIR | 0755,
					},
				},
			},
		}, nil)
	}).Return(int64(1))

	// Create a pending operation directly
	ctx := context.Background()
	operationWasCompleted := false
	inode.asyncOperation(ctx, &proto.OperationRequest{
		Operation: &proto.OperationRequest_Mkdir{
			Mkdir: &proto.MkdirRequest{
				Name: "testdir",
				Stat: emptyFileStatProto(ctx, syscall.S_IFDIR|0755),
			},
		},
	}, func(response *proto.OperationResponse, err error) {
		operationWasCompleted = true
		assert.Nil(t, err)
		assert.NotNil(t, response)

		mkdirResp, ok := response.Response.(*proto.OperationResponse_Mkdir)
		assert.True(t, ok)
		assert.Equal(t, newDirCookie, mkdirResp.Mkdir.Cookie)
	})

	// Verify pending ops were stored
	assert.Equal(t, 1, len(inode.pendingOps))
	assert.Nil(t, inode.cookie)
	assert.False(t, operationWasCompleted)

	// Now resolve the parent cookie and verify pending ops are processed
	inode.ResolveCookie(parentCookie)

	// Allow some time for callbacks to complete
	time.Sleep(10 * time.Millisecond)

	// Verify that operation was completed after resolving cookie
	assert.True(t, operationWasCompleted)
	assert.Equal(t, 0, len(inode.pendingOps))

	// Verify the request was sent with the correct parameters
	assert.NotNil(t, capturedRequest)
	assert.Equal(t, parentCookie, capturedRequest.Cookie)

	// Verify the mkdir operation details
	mkdirReq, ok := capturedRequest.Operation.(*proto.OperationRequest_Mkdir)
	assert.True(t, ok, "Expected Mkdir operation")
	assert.Equal(t, "testdir", mkdirReq.Mkdir.Name)
	assert.Equal(t, uint32(syscall.S_IFDIR|0755), mkdirReq.Mkdir.Stat.Mode)

	// Verify all mock expectations were met
	mockServer.AssertExpectations(t)
}

// TestMkdirOperationError tests error handling in async/sync operations
func TestMkdirOperationError(t *testing.T) {
	// Setup
	mockServer := NewMockServerProtocol()
	cookie := []byte("parent-dir-cookie")

	// Mock will track but not enforce the RegisterServerCallback call
	mockServer.On("RegisterServerCallback", mock.Anything, mock.Anything).Return()

	// Create inode
	inode := NewInode(mockServer, cookie, 0)

	// Set up the EnqueueOperation to return an error
	mockServer.On("EnqueueOperation", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		callback := args.Get(1).(OpCallback)
		// Pass both response and error to test how error handling works
		errorResponse := &proto.OperationResponse{
			Response: &proto.OperationResponse_Mkdir{
				Mkdir: &proto.MkdirResponse{},
			},
		}
		callback(errorResponse, syscall.EIO)
	}).Return(int64(1))

	// Test sync operation
	ctx := context.Background()
	response, err := inode.syncOperation(ctx, &proto.OperationRequest{
		Cookie: cookie,
		Operation: &proto.OperationRequest_Mkdir{
			Mkdir: &proto.MkdirRequest{
				Name: "testdir",
				Stat: emptyFileStatProto(ctx, syscall.S_IFDIR|0755),
			},
		},
	})

	// Verify the error was returned
	assert.Nil(t, response) // Response should be nil when error is not nil
	assert.Equal(t, syscall.EIO, err)

	// Verify all mock expectations were met
	mockServer.AssertExpectations(t)
}
