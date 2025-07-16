package vfs

import (
	"os"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"velda.io/clfs/pkg/proto"
)

// TestMkdirAsyncRequest verifies that in async mode the Mkdir method sends the correct request
func TestMkdirAsyncRequest(t *testing.T) {
	// Setup
	mockServer := NewMockServerProtocol(t)
	cookie := []byte("parent-dir-cookie")

	// Mock will track but not enforce the RegisterServerCallback call
	mockServer.On("RegisterServerCallback", mock.Anything, mock.Anything).Return()

	// Create parent inode with SYNC_EXCLUSIVE_WRITE flag (async mode)
	inode := NewDirInode(mockServer, cookie, SYNC_EXCLUSIVE_WRITE, DefaultRootStat())

	// Define new directory cookie
	newDirCookie := []byte("new-dir-cookie")
	asyncComplete := make(chan struct{})
	asyncCallbackComplete := make(chan struct{})

	// Set up the mock server's EnqueueOperation expectation and capture the request
	mockServer.On("EnqueueOperation", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		capturedRequest := args.Get(0).(*proto.OperationRequest)
		callback := args.Get(1).(OpCallback)
		switch s := capturedRequest.Operation.(type) {
		case *proto.OperationRequest_Mkdir:
			assert.Equal(t, "testdir", s.Mkdir.Name, "Expected Mkdir name to match")
			assert.Equal(t, cookie, capturedRequest.Cookie, "Expected cookie to match")
			assert.Equal(t, uint32(syscall.S_IFDIR|0755), s.Mkdir.Stat.Mode)
			go func() {
				<-asyncComplete
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
				close(asyncCallbackComplete)
			}()
		default:
			t.Fatalf("Unexpected operation type: %T", s)
		}
	}).Return(int64(1))

	dir, _ := testMount(t, inode, nil)
	err := os.Mkdir(dir+"/testdir", 0755)
	assert.NoError(t, err, "Expected Mkdir to succeed")

	close(asyncComplete)

	<-asyncCallbackComplete
	child := inode.GetChild("testdir").Operations().(*DirInode)
	assert.NotNil(t, child, "Expected child inode to be created")
	assert.Equal(t, child.cookie, newDirCookie)

	// Verify all mock expectations were met
	mockServer.AssertExpectations(t)
}

// TestMkdirSyncRequest verifies that in sync mode the Mkdir method sends the correct request
func TestMkdirSyncRequest(t *testing.T) {
	// Setup
	mockServer := NewMockServerProtocol(t)
	cookie := []byte("parent-dir-cookie")

	// Mock will track but not enforce the RegisterServerCallback call
	mockServer.On("RegisterServerCallback", mock.Anything, mock.Anything).Return()

	// Create parent inode with no sync flags (sync mode)
	inode := NewDirInode(mockServer, cookie, 0, DefaultRootStat())

	// Define new directory cookie
	newDirCookie := []byte("new-dir-cookie")

	asyncComplete := make(chan struct{})
	asyncCallbackComplete := make(chan struct{})

	mockServer.On("EnqueueOperation", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		capturedRequest := args.Get(0).(*proto.OperationRequest)
		callback := args.Get(1).(OpCallback)
		switch s := capturedRequest.Operation.(type) {
		case *proto.OperationRequest_GetAttr:
			assert.Equal(t, cookie, capturedRequest.Cookie, "Expected cookie to match")
			callback(&proto.OperationResponse{
				SeqId: capturedRequest.SeqId,
				Response: &proto.OperationResponse_GetAttr{
					GetAttr: &proto.GetAttrResponse{
						Stat: DefaultRootStat(),
					},
				},
			}, nil)
		case *proto.OperationRequest_Lookup:
			assert.Equal(t, "testdir", s.Lookup.Name, "Expected Lookup name to match")
			assert.Equal(t, cookie, capturedRequest.Cookie, "Expected cookie to match")
			callback(&proto.OperationResponse{
				SeqId: capturedRequest.SeqId,
				Response: &proto.OperationResponse_Lookup{
					Lookup: &proto.LookupResponse{},
				},
			}, nil)
			return
		case *proto.OperationRequest_Mkdir:
			assert.Equal(t, "testdir", s.Mkdir.Name, "Expected Mkdir name to match")
			assert.Equal(t, cookie, capturedRequest.Cookie, "Expected cookie to match")
			assert.Equal(t, uint32(syscall.S_IFDIR|0755), s.Mkdir.Stat.Mode)
			go func() {
				<-asyncComplete
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
				close(asyncCallbackComplete)
			}()
		default:
			t.Fatalf("Unexpected operation type: %T", s)
		}
	}).Return(int64(1))

	// Call the asyncOperation directly to test request handling
	dir, _ := testMount(t, inode, nil)
	testComplete := make(chan struct{})
	asyncCompleting := false
	go func() {
		err := os.Mkdir(dir+"/testdir", 0755)
		assert.True(t, asyncCompleting, "Expected async operation to be in progress")
		assert.NoError(t, err, "Expected Mkdir to succeed")
		close(testComplete)
	}()
	time.Sleep(50 * time.Millisecond) // Give the async operation time to start
	asyncCompleting = true
	close(asyncComplete)
	<-asyncCallbackComplete

	// Verify all mock expectations were met
	mockServer.AssertExpectations(t)
}

// TestMkdirWithPendingCookie tests the case where the parent node doesn't have a cookie yet
func TestMkdirWithPendingCookie(t *testing.T) {
	// Setup
	mockServer := NewMockServerProtocol(t)

	// Define cookies
	parentCookie := []byte("parent-dir-cookie")
	newDirCookie := []byte("new-dir-cookie")
	childDirCookie := []byte("child-dir-cookie")

	// Mock will track but not enforce the RegisterServerCallback call
	mockServer.On("RegisterServerCallback", mock.Anything, mock.Anything).Times(3).Return()

	inode := NewDirInode(mockServer, parentCookie, SYNC_EXCLUSIVE_WRITE, DefaultRootStat())

	asyncComplete := make(chan struct{})
	wg := &sync.WaitGroup{}
	wg.Add(2)

	mockServer.On("EnqueueOperation", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		// Capture the request for validation
		capturedRequest := args.Get(0).(*proto.OperationRequest)
		callback := args.Get(1).(OpCallback)

		switch s := capturedRequest.Operation.(type) {
		case *proto.OperationRequest_Mkdir:
			switch s.Mkdir.Name {
			case "testdir":
				assert.Equal(t, parentCookie, capturedRequest.Cookie, "Expected parent cookie to match")
				go func() {
					<-asyncComplete
					callback(&proto.OperationResponse{
						Response: &proto.OperationResponse_Mkdir{
							Mkdir: &proto.MkdirResponse{
								Cookie: newDirCookie,
								Stat:   s.Mkdir.Stat,
							},
						},
					}, nil)
					wg.Done()
				}()
			case "testdir2":
				assert.Equal(t, newDirCookie, capturedRequest.Cookie, "Expected new dir cookie to level 1 dir")
				callback(&proto.OperationResponse{
					Response: &proto.OperationResponse_Mkdir{
						Mkdir: &proto.MkdirResponse{
							Cookie: childDirCookie,
							Stat:   s.Mkdir.Stat,
						},
					},
				}, nil)
				wg.Done()
			}
		default:
			t.Fatalf("Unexpected operation type: %T", s)
		}
	}).Return(int64(1)).Twice()

	dir, _ := testMount(t, inode, nil)
	os.Mkdir(dir+"/testdir", 0755)
	os.Mkdir(dir+"/testdir/testdir2", 0755)

	time.Sleep(50 * time.Millisecond)
	close(asyncComplete)
	wg.Wait()

	// Verify all mock expectations were met
	mockServer.AssertExpectations(t)
}

// TestMkdirOperationError tests error handling in async/sync operations
func TestMkdirOperationError(t *testing.T) {
	// Setup
	mockServer := NewMockServerProtocol(t)
	cookie := []byte("parent-dir-cookie")

	// Mock will track but not enforce the RegisterServerCallback call
	mockServer.On("RegisterServerCallback", mock.Anything, mock.Anything).Return()

	// Create inode
	inode := NewDirInode(mockServer, cookie, 0, DefaultRootStat())

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
	dir, _ := testMount(t, inode, nil)
	err := os.Mkdir(dir+"/testdir", 0755)

	// Verify the error was returned
	assert.ErrorIs(t, err, syscall.EIO)

	// Verify all mock expectations were met
	mockServer.AssertExpectations(t)
}
