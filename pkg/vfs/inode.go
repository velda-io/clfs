package vfs

import (
	"context"
	"sync"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"google.golang.org/protobuf/types/known/timestamppb"
	"velda.io/mtfs/pkg/proto"
)

type OpCallback func(*proto.OperationResponse, error)
type ServerCallback func(*proto.OperationResponse)
type ServerProtocol interface {
	EnqueueOperation(request *proto.OperationRequest, callback OpCallback) int64
	RegisterServerCallback(cookie []byte, callback ServerCallback)
	UnregisterServerCallback(cookie []byte)
}

type pendingRequest struct {
	requestId int64
	request   *proto.OperationRequest
	callback  OpCallback
}

type Inode struct {
	fs.Inode
	serverProtocol ServerProtocol
	// Cookie is the identifier for the inode, used to communicate with the server.
	cookie     []byte
	opMu       sync.Mutex
	pendingOps []*pendingRequest // Ops waiting for the cookie to be set.
	syncer     *syncer

	cachedStat *proto.FileStat
}

func (n *Inode) init(serverProtocol ServerProtocol, cookie []byte, initialSyncGrants int, initialStat *proto.FileStat) {
	n.serverProtocol = serverProtocol
	n.cookie = cookie
	n.syncer = NewSyncer()
	n.cachedStat = initialStat
	n.syncer.flags = initialSyncGrants
	n.syncer.desiredFlags = initialSyncGrants
	if cookie != nil {
		serverProtocol.RegisterServerCallback(cookie, n.ReceiveServerRequest)
	}
}

var _ = (fs.NodeGetattrer)((*Inode)(nil))

func (n *Inode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	op := n.syncer.StartRead()
	defer n.syncer.Complete(op)
	if op.Async() {
		out.Attr = *AttrFromStatProto(n.cachedStat)
		return fs.OK
	}
	response, err := n.syncOperation(ctx, &proto.OperationRequest{
		Operation: &proto.OperationRequest_GetAttr{GetAttr: &proto.GetAttrRequest{}},
	})
	if err != nil {
		return fs.ToErrno(err)
	}
	switch s := response.Response.(type) {
	case *proto.OperationResponse_GetAttr:
		out.Attr = *AttrFromStatProto(s.GetAttr.Stat)
		n.cachedStat = s.GetAttr.Stat
		if s.GetAttr.ClaimUpdate != proto.ClaimStatus_CLAIM_STATUS_UNSPECIFIED {
			n.handleClaimUpdate(s.GetAttr.ClaimUpdate)
		}
		return fs.OK
	default:
		debugf("Received response with SeqId %d but unknown type: %T, expecting GetAttrResponse", response.SeqId, s)
		return syscall.EIO
	}
}

var _ = (fs.NodeSetattrer)((*Inode)(nil))

func (n *Inode) Setattr(ctx context.Context, fh fs.FileHandle, attr *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	op := n.syncer.StartWrite()
	defer n.syncer.Complete(op)
	if op.Async() {
		if attr.Valid&fuse.FATTR_MODE != 0 {
			n.cachedStat.Mode = attr.Mode
		}
		if attr.Valid&fuse.FATTR_UID != 0 {
			n.cachedStat.Uid = attr.Uid
		}
		if attr.Valid&fuse.FATTR_GID != 0 {
			n.cachedStat.Gid = attr.Gid
		}
		if attr.Valid&fuse.FATTR_SIZE != 0 {
			n.cachedStat.Size = attr.Size
		}
		if atime, ok := attr.GetATime(); ok {
			n.cachedStat.Atime = timestamppb.New(atime)
		}
		if mtime, ok := attr.GetMTime(); ok {
			n.cachedStat.Mtime = timestamppb.New(mtime)
		}
		if ctime, ok := attr.GetCTime(); ok {
			n.cachedStat.Ctime = timestamppb.New(ctime)
		}
		out.Attr = *AttrFromStatProto(n.cachedStat)
		n.asyncOperation(ctx, &proto.OperationRequest{
			Operation: &proto.OperationRequest_SetAttr{SetAttr: &proto.SetAttrRequest{
				Stat: n.cachedStat,
			}},
		}, func(response *proto.OperationResponse, err error) {
			defer n.syncer.CompleteAsync(op)
			if err != nil {
				return
			}
			switch s := response.Response.(type) {
			case *proto.OperationResponse_SetAttr:
				out.Attr = *AttrFromStatProto(s.SetAttr.Stat)
				n.cachedStat = s.SetAttr.Stat
			}
		})
		return fs.OK
	} else {
		// TODO: Generate proto
		response, err := n.syncOperation(ctx, &proto.OperationRequest{
			Operation: &proto.OperationRequest_SetAttr{SetAttr: &proto.SetAttrRequest{
				Stat: emptyFileStatProto(ctx, n.cachedStat.Mode),
			}},
		})
		if err != nil {
			return fs.ToErrno(err)
		}
		switch s := response.Response.(type) {
		case *proto.OperationResponse_SetAttr:
			out.Attr = *AttrFromStatProto(s.SetAttr.Stat)
			n.cachedStat = s.SetAttr.Stat
			return fs.OK
		default:
			debugf("Received response with SeqId %d but unknown type: %T, expecting SetAttrResponse", response.SeqId, s)
			return syscall.EIO
		}
	}
}

// TODO: xattr

func (n *Inode) doOperation(ctx context.Context, async bool, request *proto.OperationRequest, callback OpCallback) {
	if !async {
		completed := make(chan struct{})
		newCallback := func(response *proto.OperationResponse, err error) {
			defer close(completed)
			callback(response, err)
		}
		n.asyncOperation(ctx, request, newCallback)
		<-completed
		return
	} else {
		n.asyncOperation(ctx, request, callback)
	}
}

func (n *Inode) asyncOperation(ctx context.Context, request *proto.OperationRequest, callback OpCallback) {
	n.opMu.Lock()
	defer n.opMu.Unlock()
	operation := &pendingRequest{
		request:  request,
		callback: callback,
	}
	if n.cookie == nil && request.GetMount() == nil {
		n.pendingOps = append(n.pendingOps, operation)
		return
	}
	request.Cookie = n.cookie
	operation.requestId = n.serverProtocol.EnqueueOperation(request, callback)
}

func (n *Inode) syncOperation(ctx context.Context, request *proto.OperationRequest) (*proto.OperationResponse, error) {
	outChan := make(chan *proto.OperationResponse, 1)
	errChan := make(chan error, 1)

	n.asyncOperation(ctx, request, func(response *proto.OperationResponse, err error) {
		if err != nil {
			errChan <- err
			return
		}
		outChan <- response
	})

	select {
	case response := <-outChan:
		return response, nil
	case err := <-errChan:
		return nil, err
	}
}

func (n *Inode) ResolveCookie(cookie []byte) {
	n.opMu.Lock()
	defer n.opMu.Unlock()
	if n.cookie != nil {
		panic("Cookie already set")
	}
	n.cookie = cookie
	if len(n.pendingOps) > 0 {
		for _, op := range n.pendingOps {
			op.request.Cookie = cookie
			op.requestId = n.serverProtocol.EnqueueOperation(op.request, op.callback)
		}
	}
	n.pendingOps = nil
	n.serverProtocol.RegisterServerCallback(cookie, n.ReceiveServerRequest)
}

func (n *Inode) handleClaimUpdate(update proto.ClaimStatus) {
	switch update {
	case proto.ClaimStatus_CLAIM_STATUS_EXCLUSIVE_WRITE_GRANTED:
		n.syncer.UpgradeClaim(SYNC_EXCLUSIVE_WRITE, SYNC_EXCLUSIVE_WRITE)
	case proto.ClaimStatus_CLAIM_STATUS_LOCK_READ_GRANTED:
		n.syncer.UpgradeClaim(SYNC_LOCK_READ, SYNC_LOCK_READ)
	case proto.ClaimStatus_CLAIM_STATUS_EXCLUSIVE_WRITE_REVOKED:
		n.syncer.UpgradeClaim(SYNC_EXCLUSIVE_WRITE, 0)
	case proto.ClaimStatus_CLAIM_STATUS_LOCK_READ_REVOKED:
		n.syncer.UpgradeClaim(SYNC_LOCK_READ, 0)
	default:
		debugf("Received unknown claim update: %v", update)
	}
}

func (n *Inode) ReceiveServerRequest(response *proto.OperationResponse) {
	n.opMu.Lock()
	defer n.opMu.Unlock()
	switch t := response.ServerRequest.(type) {
	case *proto.OperationResponse_ClaimUpdate:
		n.handleClaimUpdate(t.ClaimUpdate.Status)
		return
	default:
		debugf("Received response with SeqId 0 but unknown type: %T", t)
		return
	}
}
