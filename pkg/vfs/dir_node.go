package vfs

import (
	"context"
	"log"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"velda.io/mtfs/pkg/proto"
)

type DirInode struct {
	Inode
	hasFullData bool
}

func NewDirInode(serverProtocol ServerProtocol, cookie []byte, initialSyncGrants int, initialStat *proto.FileStat) *DirInode {
	n := &DirInode{}
	n.init(serverProtocol, cookie, initialSyncGrants, initialStat)
	if initialSyncGrants != 0 {
		n.hasFullData = true
	}
	return n
}

var _ = (fs.NodeLookuper)((*DirInode)(nil))

func (n *DirInode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	op := n.syncer.StartRead()
	defer n.syncer.Complete(op)
	for op.Async() {
		child := n.GetChild(name)
		if child == nil && !n.hasFullData {
			break
		}
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

var _ = (fs.NodeMkdirer)((*DirInode)(nil))

func (n *DirInode) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	op := n.syncer.StartWrite()
	defer n.syncer.Complete(op)
	if op.Async() && n.hasFullData {
		if n.GetChild(name) != nil {
			return nil, syscall.EEXIST
		}

		newFileStat(ctx, &out.Attr, mode|syscall.S_IFDIR)
		stat := StatProtoFromAttr(&out.Attr)
		node := NewDirInode(n.serverProtocol, nil, SYNC_EXCLUSIVE_WRITE, stat)
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
			node := NewDirInode(n.serverProtocol, s.Mkdir.Cookie, SYNC_EXCLUSIVE_WRITE, s.Mkdir.Stat)
			return n.NewInode(ctx, node, fs.StableAttr{Mode: mode | syscall.S_IFDIR}), 0
		default:
			log.Printf("Received response with SeqId %d but unknown type: %T, expecting MkdirResponse", response.SeqId, s)
			return nil, syscall.EIO
		}
	}
}

var _ = (fs.NodeMknoder)((*DirInode)(nil))

func (n *DirInode) Mknod(ctx context.Context, name string, mode uint32, dev uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	op := n.syncer.StartWrite()
	defer n.syncer.Complete(op)
	if op.Async() && n.hasFullData {
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

var _ = (fs.NodeRmdirer)((*DirInode)(nil))

func (n *DirInode) Rmdir(ctx context.Context, name string) syscall.Errno {
	op := n.syncer.StartWrite()
	defer n.syncer.Complete(op)
	for op.Async() {
		child := n.GetChild(name)
		if child == nil && !n.hasFullData {
			// Sync
			break
		}
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
	}
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

var _ = (fs.NodeRenamer)((*DirInode)(nil))

func (n *DirInode) Rename(ctx context.Context, oldName string, newDir fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	return fs.OK
}

var _ = (fs.NodeUnlinker)((*DirInode)(nil))

func (n *DirInode) Unlink(ctx context.Context, name string) syscall.Errno {
	op := n.syncer.StartWrite()
	defer n.syncer.Complete(op)
	if op.Async() {
		child := n.GetChild(name)
		if child == nil && !n.hasFullData {
			// Sync
			return syscall.ENOENT
		}
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

var _ = (fs.NodeSymlinker)((*DirInode)(nil))

func (n *DirInode) Symlink(ctx context.Context, pointedTo string, linkName string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	op := n.syncer.StartWrite()
	defer n.syncer.Complete(op)
	for op.Async() {
		child := n.GetChild(linkName)
		if child == nil && !n.hasFullData {
			// Sync
			break
		}
		if child != nil {
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
	}
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
		node := NewLinkNode(n.serverProtocol, s.Symlink.Cookie, 0, []byte(pointedTo), s.Symlink.Stat)
		return n.NewInode(ctx, node, fs.StableAttr{Mode: syscall.S_IFLNK | 0777}), 0
	default:
		log.Printf("Received response with SeqId %d but unknown type: %T, expecting SymlinkResponse", response.SeqId, s)
		return nil, syscall.EIO
	}
}

var _ = (fs.NodeCreater)((*DirInode)(nil))

func (n *DirInode) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	op := n.syncer.StartWrite()
	defer n.syncer.Complete(op)
	if op.Async() && n.hasFullData {
		child := n.GetChild(name)
		if child != nil && flags&syscall.O_EXCL != 0 {
			return nil, nil, 0, syscall.EEXIST
		}
		if child != nil {
			// TODO: Reopen?
			return nil, nil, 0, syscall.EEXIST
		}
		newFileStat(ctx, &out.Attr, mode)
		stat := StatProtoFromAttr(&out.Attr)
		node := NewFileInode(n.serverProtocol, nil, SYNC_EXCLUSIVE_WRITE, stat)

		fh := &fileHandle{
			handle: nil,
			flags:  flags,
		}
		n.asyncOperation(ctx, &proto.OperationRequest{
			Operation: &proto.OperationRequest_Create{Create: &proto.CreateRequest{
				Name:  name,
				Stat:  stat,
				Flags: flags,
			}},
		}, func(response *proto.OperationResponse, err error) {
			defer n.syncer.CompleteAsync(op)
			if err != nil {
				log.Printf("Async Create failed: %v", err)
				return
			}
			switch s := response.Response.(type) {
			case *proto.OperationResponse_Create:
				cookie := s.Create.Cookie
				node.ResolveCookie(cookie)
				fh.SetHandle(s.Create.FileHandle)
			default:
				log.Printf("Received unexpected response type: %T", s)
				return
			}
		})
		return n.NewInode(ctx, node, fs.StableAttr{Mode: mode}), fh, 0, fs.OK
	} else {
		response, err := n.syncOperation(ctx, &proto.OperationRequest{
			Cookie: n.cookie,
			Operation: &proto.OperationRequest_Create{Create: &proto.CreateRequest{
				Name:  name,
				Stat:  emptyFileStatProto(ctx, mode),
				Flags: flags,
			}},
		})
		if err != nil {
			return nil, nil, 0, fs.ToErrno(err)
		}
		switch s := response.Response.(type) {
		case *proto.OperationResponse_Create:
			out.Attr = *AttrFromStatProto(s.Create.Stat)
			node := NewFileInode(n.serverProtocol, s.Create.Cookie, 0, s.Create.Stat)
			node.handleClaimUpdate(s.Create.Claim)
			fh := &fileHandle{
				handle: s.Create.FileHandle,
				flags:  flags,
			}
			return n.NewInode(ctx, node, fs.StableAttr{Mode: mode}), fh, 0, fs.OK
		default:
			log.Printf("Received response with SeqId %d but unknown type: %T, expecting CreateResponse", response.SeqId, s)
			return nil, nil, 0, syscall.EIO
		}
	}
}

func (n *DirInode) Mount(ctx context.Context, volume string) error {
	op := n.syncer.StartWrite()
	defer n.syncer.Complete(op)
	// Mount is always synchronous
	response, err := n.syncOperation(ctx, &proto.OperationRequest{
		Operation: &proto.OperationRequest_Mount{Mount: &proto.MountRequest{
			Volume: volume,
		}},
	})
	if err != nil {
		return fs.ToErrno(err)
	}
	switch s := response.Response.(type) {
	case *proto.OperationResponse_Mount:
		if s.Mount.Cookie == nil {
			return syscall.EINVAL // Mount failed, no cookie returned
		}
		n.ResolveCookie(s.Mount.Cookie)
		n.handleClaimUpdate(s.Mount.Claim)
		n.cachedStat = s.Mount.Stat
	}
	return nil
}

var _ = (fs.NodeOpendirHandler)((*DirInode)(nil))

func (n *DirInode) OpendirHandle(ctx context.Context, flags uint32) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	// TODO: Exclusive read?
	op := n.syncer.StartRead()
	if op.Async() && n.hasFullData {
		defer n.syncer.Complete(op)
		allChildren := n.Children()
		list := make([]fuse.DirEntry, 0, len(allChildren))
		for name, child := range allChildren {
			list = append(list, fuse.DirEntry{
				Ino:  uint64(child.StableAttr().Ino),
				Name: name,
				Mode: child.Mode(),
				Off:  0, // Offset is not used in this context
			})
		}
		return &cachedDirStream{
			idx:     0,
			entries: list,
			op:      op,
			inode:   n,
		}, 0, 0
	}
	return &DirStream{inode: n, op: op}, 0, 0
}

type DirStreamCloser struct {
	DirStream
	op *operation
}

type DirStream struct {
	inode       *DirInode
	offsetToken []byte // Token for the directory scan
	op          *operation
	returned    int
	fetched     []*proto.DirEntry // Last fetched directory entries
}

var _ = (fs.FileReaddirenter)((*DirStream)(nil))

func (s *DirStream) Readdirent(ctx context.Context) (*fuse.DirEntry, syscall.Errno) {
	for s.returned >= len(s.fetched) && s.op != nil {
		response, err := s.inode.syncOperation(ctx, &proto.OperationRequest{
			Cookie: s.inode.cookie,
			Operation: &proto.OperationRequest_ScanDir{ScanDir: &proto.ScanDirectoryRequest{
				Token: s.offsetToken,
			}},
		})
		if err != nil {
			return nil, fs.ToErrno(err)
		}
		switch resp := response.Response.(type) {
		case *proto.OperationResponse_ScanDir:
			s.fetched = append(s.fetched, resp.ScanDir.Entries...)
			s.offsetToken = resp.ScanDir.OffsetToken
			// TODO: Set the node to have full data.
			if s.offsetToken == nil {
				s.inode.syncer.Complete(s.op)
				s.op = nil
			}
		default:
			log.Printf("Received unexpected response type: %T", resp)
			return nil, syscall.EIO
		}
	}
	if s.returned >= len(s.fetched) {
		return nil, fs.OK // No more entries to return
	}
	entry := s.fetched[s.returned]
	s.returned++

	out := &fuse.DirEntry{
		Ino:  uint64(entry.Inode),
		Name: entry.Name,
		Mode: entry.Type,
		Off:  entry.Offset,
	}
	return out, fs.OK
}

var _ = (fs.FileReleasedirer)((*DirStream)(nil))

func (s *DirStream) Releasedir(ctx context.Context, flags uint32) {
	if s.op != nil {
		s.inode.syncer.Complete(s.op)
		s.op = nil
	}
}

type cachedDirStream struct {
	idx     int
	entries []fuse.DirEntry
	op      *operation
	inode   *DirInode
}

func (a *cachedDirStream) Releasedir(ctx context.Context, releaseFlags uint32) {
	if a.op != nil {
		a.inode.syncer.Complete(a.op)
		a.op = nil
	}
}

func (a *cachedDirStream) Readdirent(ctx context.Context) (de *fuse.DirEntry, errno syscall.Errno) {
	if a.idx >= len(a.entries) {
		return nil, 0 // No more entries
	}
	e := a.entries[a.idx]
	a.idx++
	e.Off = uint64(a.idx)
	return &e, 0
}
