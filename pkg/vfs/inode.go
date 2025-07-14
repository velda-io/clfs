package vfs

import (
	"context"
	"log"
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

func NewInode(serverProtocol ServerProtocol, cookie []byte, initialSyncGrants int, initialStat *proto.FileStat) *Inode {
	n := &Inode{}
	n.init(serverProtocol, cookie, initialSyncGrants, initialStat)
	return n
}

func (n *Inode) init(serverProtocol ServerProtocol, cookie []byte, initialSyncGrants int, initialStat *proto.FileStat) {
	n.serverProtocol = serverProtocol
	n.cookie = cookie
	n.syncer = NewSyncer()
	n.cachedStat = initialStat
	n.syncer.flags = initialSyncGrants
	if cookie != nil {
		serverProtocol.RegisterServerCallback(cookie, n.ReceiveResponse)
	}
}

var _ = (fs.NodeLookuper)((*Inode)(nil))

func (n *Inode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	op := n.syncer.StartRead()
	defer n.syncer.Complete(op)
	if op.Async() {
		child := n.GetChild(name)
		if child == nil {
			return nil, syscall.ENOENT
		}

		if ga, ok := child.Operations().(fs.NodeGetattrer); ok {
			var a fuse.AttrOut
			errno := ga.Getattr(ctx, nil, &a)
			if errno == 0 {
				out.Attr = a.Attr
			}
		}
		return child, fs.OK
	}
	response, err := n.syncOperation(ctx, &proto.OperationRequest{
		Cookie: n.cookie,
		Operation: &proto.OperationRequest_Lookup{Lookup: &proto.LookupRequest{
			Name: name,
		}},
	})
	if err != nil {
		return nil, fs.ToErrno(err)
	}
	switch s := response.Response.(type) {
	case *proto.OperationResponse_Lookup:
		if s.Lookup.Cookie == nil {
			return nil, syscall.ENOENT
		}
		out.Attr = *AttrFromStatProto(s.Lookup.Stat)
		node := NewInode(n.serverProtocol, s.Lookup.Cookie, 0, s.Lookup.Stat)
		if s.Lookup.Claim != proto.ClaimStatus_CLAIM_STATUS_UNSPECIFIED {
			node.handleClaimUpdate(s.Lookup.Claim)
		}
		return n.NewInode(ctx, node, fs.StableAttr{Mode: s.Lookup.Stat.Mode}), 0
	default:
		log.Printf("Received response with SeqId %d but unknown type: %T, expecting LookupResponse", response.SeqId, s)
		return nil, syscall.EIO
	}
}

var _ = (fs.NodeMkdirer)((*Inode)(nil))

func (n *Inode) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	op := n.syncer.StartWrite()
	defer n.syncer.Complete(op)
	if op.Async() {
		if n.GetChild(name) != nil {
			return nil, syscall.EEXIST
		}

		newFileStat(ctx, &out.Attr, mode|syscall.S_IFDIR)
		stat := StatProtoFromAttr(&out.Attr)
		node := NewInode(n.serverProtocol, nil, SYNC_EXCLUSIVE_WRITE, stat)
		n.asyncOperation(
			ctx,
			&proto.OperationRequest{
				Operation: &proto.OperationRequest_Mkdir{Mkdir: &proto.MkdirRequest{
					Name: name,
					Stat: stat,
				}},
			},
			func(response *proto.OperationResponse, err error) {
				defer n.syncer.CompleteAsync(op)
				if err != nil {
					return
				}
				switch s := response.Response.(type) {
				case *proto.OperationResponse_Mkdir:
					cookie := s.Mkdir.Cookie
					node.ResolveCookie(cookie)
				default:
					// Handle unexpected response type
					return
				}
			},
		)
		return n.NewInode(ctx, node, fs.StableAttr{Mode: mode | syscall.S_IFDIR}), 0
	} else {
		response, err := n.syncOperation(ctx, &proto.OperationRequest{
			Cookie: n.cookie,
			Operation: &proto.OperationRequest_Mkdir{Mkdir: &proto.MkdirRequest{
				Name: name,
				Stat: emptyFileStatProto(ctx, mode|syscall.S_IFDIR),
			}},
		})
		if err != nil {
			return nil, fs.ToErrno(err)
		}
		switch s := response.Response.(type) {
		case *proto.OperationResponse_Mkdir:
			out.Attr = *AttrFromStatProto(s.Mkdir.Stat)
			node := NewInode(n.serverProtocol, s.Mkdir.Cookie, SYNC_EXCLUSIVE_WRITE, s.Mkdir.Stat)
			return n.NewInode(ctx, node, fs.StableAttr{Mode: mode | syscall.S_IFDIR}), 0
		default:
			log.Printf("Received response with SeqId %d but unknown type: %T, expecting MkdirResponse", response.SeqId, s)
			return nil, syscall.EIO
		}
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
		log.Printf("Received response with SeqId %d but unknown type: %T, expecting GetAttrResponse", response.SeqId, s)
		return syscall.EIO
	}
}

var _ = (fs.NodeMknoder)((*Inode)(nil))

func (n *Inode) Mknod(ctx context.Context, name string, mode uint32, dev uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	op := n.syncer.StartWrite()
	defer n.syncer.Complete(op)
	if op.Async() {
		if n.GetChild(name) != nil {
			return nil, syscall.EEXIST
		}
		newFileStat(ctx, &out.Attr, mode)
		out.Attr.Rdev = dev
		stat := StatProtoFromAttr(&out.Attr)
		node := NewInode(n.serverProtocol, nil, SYNC_EXCLUSIVE_WRITE, stat)
		n.asyncOperation(ctx, &proto.OperationRequest{
			Operation: &proto.OperationRequest_Mknod{Mknod: &proto.MknodRequest{
				Name: name,
				Stat: stat,
			}},
		}, func(response *proto.OperationResponse, err error) {
			defer n.syncer.CompleteAsync(op)
			if err != nil {
				return
			}
			switch s := response.Response.(type) {
			case *proto.OperationResponse_Mknod:
				cookie := s.Mknod.Cookie
				node.ResolveCookie(cookie)
			default:
				// Handle unexpected response type
				return
			}
		})
		return n.NewInode(ctx, node, fs.StableAttr{Mode: mode}), 0
	} else {
		response, err := n.syncOperation(ctx, &proto.OperationRequest{
			Cookie: n.cookie,
			Operation: &proto.OperationRequest_Mknod{Mknod: &proto.MknodRequest{
				Name: name,
				Stat: emptyFileStatProto(ctx, mode),
			}},
		})
		if err != nil {
			return nil, fs.ToErrno(err)
		}
		switch s := response.Response.(type) {
		case *proto.OperationResponse_Mknod:
			out.Attr = *AttrFromStatProto(s.Mknod.Stat)
			node := NewInode(n.serverProtocol, s.Mknod.Cookie, SYNC_EXCLUSIVE_WRITE, s.Mknod.Stat)
			return n.NewInode(ctx, node, fs.StableAttr{Mode: mode}), 0
		default:
			log.Printf("Received response with SeqId %d but unknown type: %T, expecting MknodResponse", response.SeqId, s)
			return nil, syscall.EIO
		}
	}
}

var _ = (fs.NodeRmdirer)((*Inode)(nil))

func (n *Inode) Rmdir(ctx context.Context, name string) syscall.Errno {
	op := n.syncer.StartWrite()
	defer n.syncer.Complete(op)
	if op.Async() {
		child := n.GetChild(name)
		if child == nil {
			return syscall.ENOENT
		}
		childNode, ok := child.Operations().(*Inode)
		if !ok {
			return syscall.EINVAL
		}
		checkChildEmpty := func() bool {
			op := childNode.syncer.StartRead()
			defer childNode.syncer.Complete(op)
			if op.Async() {
				return len(child.Children()) == 0
			}
			// TODO: ReadDir
			return true
		}()
		if !checkChildEmpty {
			return syscall.ENOTEMPTY
		}

		n.asyncOperation(ctx, &proto.OperationRequest{
			Operation: &proto.OperationRequest_Rmdir{Rmdir: &proto.RmdirRequest{
				Name: name,
			}},
		}, func(response *proto.OperationResponse, err error) {
			defer n.syncer.CompleteAsync(op)
			if err != nil {
				log.Printf("Async Rmdir failed: %v", err)
				return
			}
		})
		n.RmChild(name)
		return fs.OK
	} else {
		_, err := n.syncOperation(ctx, &proto.OperationRequest{
			Operation: &proto.OperationRequest_Rmdir{Rmdir: &proto.RmdirRequest{
				Name: name,
			}},
		})
		if err != nil {
			return fs.ToErrno(err)
		}
		n.RmChild(name)
		return fs.OK
	}
}

var _ = (fs.NodeUnlinker)((*Inode)(nil))

func (n *Inode) Unlink(ctx context.Context, name string) syscall.Errno {
	op := n.syncer.StartWrite()
	defer n.syncer.Complete(op)
	if op.Async() {
		child := n.GetChild(name)
		if child == nil {
			return syscall.ENOENT
		}
		childNode, ok := child.Operations().(*Inode)
		if !ok {
			return syscall.EINVAL
		}
		if childNode.cachedStat.Mode&syscall.S_IFDIR != 0 {
			return syscall.EISDIR // Cannot unlink a directory
		}
		n.asyncOperation(ctx, &proto.OperationRequest{
			Operation: &proto.OperationRequest_Unlink{Unlink: &proto.UnlinkRequest{
				Name: name,
			}},
		}, func(response *proto.OperationResponse, err error) {
			defer n.syncer.CompleteAsync(op)
			if err != nil {
				log.Printf("Async Unlink failed: %v", err)
				return
			}
		})
		return fs.OK
	} else {
		_, err := n.syncOperation(ctx, &proto.OperationRequest{
			Operation: &proto.OperationRequest_Unlink{Unlink: &proto.UnlinkRequest{
				Name: name,
			}},
		})
		if err != nil {
			return fs.ToErrno(err)
		}
		return fs.OK
	}
}

// TODO: Rename, Create, Open

var _ = (fs.NodeOpener)((*Inode)(nil))

func (n *Inode) Open(ctx context.Context, flags uint32) (fh fs.FileHandle, fuseFlags uint32, err syscall.Errno) {
	op := n.syncer.StartWrite()
	defer n.syncer.Complete(op)
	if op.Async() {

	}
	return nil, 0, syscall.ENOSYS // Not implemented yet
}

var _ = (fs.NodeSymlinker)((*Inode)(nil))

func (n *Inode) Symlink(ctx context.Context, pointedTo string, linkName string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	op := n.syncer.StartWrite()
	defer n.syncer.Complete(op)
	if op.Async() {
		if n.GetChild(linkName) != nil {
			return nil, syscall.EEXIST
		}
		newFileStat(ctx, &out.Attr, syscall.S_IFLNK|0777) // Symlink mode
		stat := StatProtoFromAttr(&out.Attr)
		node := NewLinkNode(n.serverProtocol, nil, SYNC_LOCK_READ, []byte(pointedTo), stat)
		n.asyncOperation(ctx, &proto.OperationRequest{
			Operation: &proto.OperationRequest_Symlink{Symlink: &proto.SymlinkRequest{
				Name:   linkName,
				Target: pointedTo,
				Stat:   stat, // Include stat for symlink
			}},
		}, func(response *proto.OperationResponse, err error) {
			defer n.syncer.CompleteAsync(op)
			if err != nil {
				log.Printf("Async Symlink failed: %v", err)
				return
			}
			switch s := response.Response.(type) {
			case *proto.OperationResponse_Symlink:
				cookie := s.Symlink.Cookie
				node.ResolveCookie(cookie)
			default:
				log.Printf("Received unexpected response type: %T", s)
				return
			}
		})
		return n.NewInode(ctx, node, fs.StableAttr{Mode: syscall.S_IFLNK | 0777}), 0
	} else {
		// TODO: Owner of symlink
		response, err := n.syncOperation(ctx, &proto.OperationRequest{
			Cookie: n.cookie,
			Operation: &proto.OperationRequest_Symlink{Symlink: &proto.SymlinkRequest{
				Name:   linkName,
				Target: pointedTo,
			}},
		})
		if err != nil {
			return nil, fs.ToErrno(err)
		}
		switch s := response.Response.(type) {
		case *proto.OperationResponse_Symlink:
			out.Attr = *AttrFromStatProto(s.Symlink.Stat)
			node := NewInode(n.serverProtocol, s.Symlink.Cookie, 0, s.Symlink.Stat)
			return n.NewInode(ctx, node, fs.StableAttr{Mode: syscall.S_IFLNK | 0777}), 0
		default:
			log.Printf("Received response with SeqId %d but unknown type: %T, expecting SymlinkResponse", response.SeqId, s)
			return nil, syscall.EIO
		}
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
				log.Printf("Async Setattr failed: %v", err)
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
			log.Printf("Received response with SeqId %d but unknown type: %T, expecting SetAttrResponse", response.SeqId, s)
			return syscall.EIO
		}
	}
}

// TODO: xattr

func (n *Inode) asyncOperation(ctx context.Context, request *proto.OperationRequest, callback OpCallback) {
	n.opMu.Lock()
	defer n.opMu.Unlock()
	operation := &pendingRequest{
		request:  request,
		callback: callback,
	}
	if n.cookie == nil {
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
	n.serverProtocol.RegisterServerCallback(cookie, n.ReceiveResponse)
}

func (n *Inode) handleClaimUpdate(update proto.ClaimStatus) {
	switch update {
	case proto.ClaimStatus_CLAIM_STATUS_EXCLUSIVE_WRITE_GRANTED:
		n.syncer.UpgradeClaim(SYNC_EXCLUSIVE_WRITE_GRANTED)
	case proto.ClaimStatus_CLAIM_STATUS_LOCK_READ_GRANTED:
		n.syncer.UpgradeClaim(SYNC_LOCK_READ_GRANTED)
	case proto.ClaimStatus_CLAIM_STATUS_EXCLUSIVE_WRITE_REVOKED:
	// TODO
	case proto.ClaimStatus_CLAIM_STATUS_LOCK_READ_REVOKED:
		// TODO
	}
}

func (n *Inode) ReceiveResponse(response *proto.OperationResponse) {
	n.opMu.Lock()
	defer n.opMu.Unlock()
	switch t := response.Response.(type) {
	case *proto.OperationResponse_ClaimUpdate:
		n.handleClaimUpdate(t.ClaimUpdate.Status)
		return
	default:
		log.Printf("Received response with SeqId 0 but unknown type: %T", t)
		return
	}
}
