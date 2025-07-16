package vfs

import (
	"context"
	"sync"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"velda.io/mtfs/pkg/proto"
)

type FileInode struct {
	Inode
	cache Cache
}

func NewFileInode(serverProtocol ServerProtocol, cookie []byte, initialSyncGrants int, initialStat *proto.FileStat) *FileInode {
	n := &FileInode{}
	n.init(serverProtocol, cookie, initialSyncGrants, initialStat)
	n.syncer.SetOnRevoked(n)
	return n
}

var _ = (fs.NodeOpener)((*FileInode)(nil))
var _ = (fs.NodeReader)((*FileInode)(nil))
var _ = (fs.NodeWriter)((*FileInode)(nil))
var _ = (fs.NodeFlusher)((*FileInode)(nil))
var _ = (fs.NodeFsyncer)((*FileInode)(nil))
var _ = (fs.NodeReleaser)((*FileInode)(nil))
var _ = (hasOnRevoked)((*FileInode)(nil))

func (n *FileInode) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	var op *operation
	if flags&(syscall.O_WRONLY|syscall.O_RDWR|syscall.O_APPEND) == 0 {
		op = n.syncer.StartRead()
	} else {
		op = n.syncer.StartWrite()
	}
	defer n.syncer.Complete(op)
	fh := &fileHandle{
		handle: nil,
		flags:  flags,
	}
	var errno syscall.Errno
	n.doOperation(ctx, op.Async(), &proto.OperationRequest{
		Operation: &proto.OperationRequest_Open{
			Open: &proto.OpenRequest{
				Flags: flags,
			},
		},
	}, func(response *proto.OperationResponse, err error) {
		if err != nil {
			errno = fs.ToErrno(err)
			fh = nil
			return
		}
		switch resp := response.Response.(type) {
		case *proto.OperationResponse_Open:
			fh.SetHandle(resp.Open.FileHandle)
		default:
			debugf("Open: Unexpected response type: %T", resp)
		}
	})
	return fh, 0, errno
}

func (n *FileInode) Read(ctx context.Context, fh fs.FileHandle, dest []byte, offset int64) (fuse.ReadResult, syscall.Errno) {
	var op *operation
	op = n.syncer.StartRead()
	defer n.syncer.Complete(op)

	if op.Async() {
		r := n.cache.Read(offset, len(dest))
		if len(r) > 0 {
			return fuse.ReadResultData(r), fs.OK
		}
	}
	lfh := fh.(*fileHandle)
	lfh.WaitHandle() // Ensure the handle is ready before proceeding
	response, err := n.syncOperation(ctx, &proto.OperationRequest{
		Operation: &proto.OperationRequest_Read{
			Read: &proto.ReadRequest{
				FileHandle: lfh.handle,
				Offset:     offset,
				Size:       uint32(len(dest)),
			},
		},
	})
	if err != nil {
		return nil, fs.ToErrno(err)
	}
	switch resp := response.Response.(type) {
	case *proto.OperationResponse_Read:
		return fuse.ReadResultData(resp.Read.Data), fs.OK
	default:
		debugf("Read: Unexpected response type: %T", resp)
	}
	return nil, syscall.EIO
}

func (n *FileInode) Write(ctx context.Context, fh fs.FileHandle, data []byte, offset int64) (uint32, syscall.Errno) {
	var op *operation
	op = n.syncer.StartWrite()
	defer n.syncer.Complete(op)

	if op.Async() {
		n.cache.Insert(offset, data)
		n.syncer.StartAsync(op)
	}
	newSize := uint64(offset + int64(len(data)))
	if n.cachedStat.Size < newSize {
		n.cachedStat.Size = newSize
	}
	// Always write-ahead
	lfh := fh.(*fileHandle)
	lfh.Wait.Add(1)
	lfh.OnReady(func(handle []byte, err error) {
		n.asyncOperation(
			ctx,
			&proto.OperationRequest{
				Operation: &proto.OperationRequest_Write{
					Write: &proto.WriteRequest{
						FileHandle: lfh.handle,
						Data:       data,
						Offset:     offset,
					},
				},
			},
			func(response *proto.OperationResponse, err error) {
				if op.Async() {
					n.syncer.CompleteAsync(op)
					if err != nil {
						n.serverProtocol.ReportAsyncError("Async Write failed: %v", err)
					}
				}
				defer lfh.Wait.Done()
				if err != nil {
					lfh.writeErr = err
					return
				}
			},
		)
	})
	return uint32(len(data)), fs.OK
}

func (n *FileInode) Flush(ctx context.Context, fh fs.FileHandle) syscall.Errno {
	lfh := fh.(*fileHandle)
	lfh.Wait.Wait() // Wait for any pending writes to complete
	if lfh.writeErr != nil {
		return fs.ToErrno(lfh.writeErr)
	}

	return fs.OK
}

func (n *FileInode) Fsync(ctx context.Context, fh fs.FileHandle, flags uint32) syscall.Errno {
	lfh := fh.(*fileHandle)
	lfh.Wait.Wait() // Wait for any pending writes to complete
	if lfh.writeErr != nil {
		return fs.ToErrno(lfh.writeErr)
	}
	return fs.OK
}

func (n *FileInode) Release(ctx context.Context, fh fs.FileHandle) syscall.Errno {
	lfh := fh.(*fileHandle)
	if lfh.handle == nil {
		return fs.OK // Nothing to release
	}
	lfh.OnReady(func(handle []byte, err error) {
		lfh.Wait.Wait()
		n.asyncOperation(
			ctx,
			&proto.OperationRequest{
				Operation: &proto.OperationRequest_Close{
					Close: &proto.CloseRequest{
						FileHandle: handle,
					},
				},
			},
			func(response *proto.OperationResponse, err error) {
				if err != nil {
					debugf("Release: Close operation failed: %v", err)
				}
			},
		)
	})

	return fs.OK
}

func (n *FileInode) OnRevoked(lastFlag int) {
	n.cache.Clear()
	n.serverProtocol.UnregisterServerCallback(n.cookie)
}

type fileCallback func(handle []byte, err error)
type fileHandle struct {
	fs.FileHandle
	mu         sync.Mutex
	flags      uint32
	handle     []byte
	pendingOps []fileCallback
	Wait       sync.WaitGroup
	writeErr   error
}

func (fh *fileHandle) OnReady(callback fileCallback) {
	fh.mu.Lock()
	defer fh.mu.Unlock()
	if fh.handle != nil {
		callback(fh.handle, nil)
		return
	}
	fh.pendingOps = append(fh.pendingOps, callback)
}

func (fh *fileHandle) SetHandle(handle []byte) {
	fh.mu.Lock()
	defer fh.mu.Unlock()
	fh.handle = handle
	for _, cb := range fh.pendingOps {
		cb(handle, nil)
	}
	fh.pendingOps = nil
}

func (fh *fileHandle) WaitHandle() {
	c := make(chan struct{})
	fh.OnReady(func(handle []byte, err error) {
		close(c)
	})
	<-c
}
