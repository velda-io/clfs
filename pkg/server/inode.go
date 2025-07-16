package server

import (
	"bytes"
	"fmt"
	"unsafe"

	"golang.org/x/sys/unix"
	"velda.io/clfs/pkg/proto"
)

const (
	FATTR_MODE  = 1 << 0
	FATTR_UID   = 1 << 1
	FATTR_GID   = 1 << 2
	FATTR_SIZE  = 1 << 3
	FATTR_ATIME = 1 << 4
	FATTR_MTIME = 1 << 5
)

// ServerNode represents a file or directory on the server.
type ServerNode struct {
	volume  *Volume
	fh      unix.FileHandle
	fd      int // File descriptor for the node
	nodeId  uint64
	tracker *claimTracker // Claim tracker for this node
}

func (n *ServerNode) Close() {
	if n.fd != -1 {
		unix.Close(n.fd)
		n.fd = -1
	}
}

type HandleCallback func(*proto.OperationResponse, error)

func (n *ServerNode) Handle(s *session, req *proto.OperationRequest, callback HandleCallback) {

	do := func() {
		resp, err := n.handle(s, req)
		callback(resp, err)
	}
	var readonly bool
	switch op := req.Operation.(type) {
	// Readonly
	case *proto.OperationRequest_Lookup:
	case *proto.OperationRequest_GetAttr:
	case *proto.OperationRequest_Readlink:
	case *proto.OperationRequest_ScanDir:
	case *proto.OperationRequest_Read:
		readonly = true

	case *proto.OperationRequest_SetAttr:
	case *proto.OperationRequest_Mknod:
	case *proto.OperationRequest_Mkdir:
	case *proto.OperationRequest_Rmdir:
	case *proto.OperationRequest_Unlink:
	case *proto.OperationRequest_Symlink:
	case *proto.OperationRequest_Create:
	case *proto.OperationRequest_Write:
	case *proto.OperationRequest_Close:
	case *proto.OperationRequest_Mount:
		readonly = false

	case *proto.OperationRequest_Open:
		readonly = op.Open.Flags&(unix.O_WRONLY|unix.O_RDWR|unix.O_APPEND) == 0
	default:
		callback(nil, fmt.Errorf("unhandled request: %T", op))
		return
	}
	if readonly {
		n.tracker.Read(s, do)
	} else {
		n.tracker.Write(s, do)
	}
}

// Handle processes a single operation request for this node.
func (n *ServerNode) handle(s *session, req *proto.OperationRequest) (*proto.OperationResponse, error) {
	switch op := req.Operation.(type) {
	case *proto.OperationRequest_Lookup:
		return n.Lookup(s, op.Lookup)
	case *proto.OperationRequest_GetAttr:
		return n.GetAttr(s, op.GetAttr)
	case *proto.OperationRequest_SetAttr:
		return n.SetAttr(s, op.SetAttr)
	case *proto.OperationRequest_Mknod:
		return n.Mknod(s, op.Mknod)
	case *proto.OperationRequest_Mkdir:
		return n.Mkdir(s, op.Mkdir)
	case *proto.OperationRequest_Rmdir:
		return n.Rmdir(s, op.Rmdir)
	case *proto.OperationRequest_Unlink:
		return n.Unlink(s, op.Unlink)
	case *proto.OperationRequest_Symlink:
		return n.Symlink(s, op.Symlink)
	case *proto.OperationRequest_Readlink:
		return n.Readlink(s, op.Readlink)
	case *proto.OperationRequest_ScanDir:
		return n.ScanDirectory(s, op.ScanDir)
	case *proto.OperationRequest_Create:
		return n.Create(s, op.Create)
	case *proto.OperationRequest_Open:
		return n.Open(s, op.Open)
	case *proto.OperationRequest_Read:
		return n.Read(s, op.Read)
	case *proto.OperationRequest_Write:
		return n.Write(s, op.Write)
	case *proto.OperationRequest_Close:
		return n.CloseOp(s, op.Close)
	case *proto.OperationRequest_Mount:
		return n.Mount(s, op.Mount)
	default:
		return nil, fmt.Errorf("unhandled request: %T", op)
	}
}

func (n *ServerNode) Lookup(s *session, req *proto.LookupRequest) (*proto.OperationResponse, error) {
	child, err := n.volume.GetNode(n.fd, req.Name)
	if err != nil {
		return nil, err
	}
	var stat unix.Stat_t
	if err := unix.Fstat(child.fd, &stat); err != nil {
		child.Close()
		return nil, err
	}
	cookie := s.GetCookie(child)
	return &proto.OperationResponse{
		Response: &proto.OperationResponse_Lookup{
			Lookup: &proto.LookupResponse{
				Cookie: cookie,
				Stat:   StatProtoFromSysStat(&stat),
			},
		},
	}, nil
}

func (n *ServerNode) GetAttr(s *session, req *proto.GetAttrRequest) (*proto.OperationResponse, error) {
	var stat unix.Stat_t
	if err := unix.Fstat(n.fd, &stat); err != nil {
		return nil, err
	}
	return &proto.OperationResponse{
		Response: &proto.OperationResponse_GetAttr{
			GetAttr: &proto.GetAttrResponse{
				Stat: StatProtoFromSysStat(&stat),
			},
		},
	}, nil
}

func (n *ServerNode) Mount(s *session, req *proto.MountRequest) (*proto.OperationResponse, error) {
	if n.volume.root == nil {
		return nil, fmt.Errorf("volume root not set")
	}
	var stat unix.Stat_t
	if err := unix.Fstat(n.fd, &stat); err != nil {
		return nil, err
	}
	return &proto.OperationResponse{
		Response: &proto.OperationResponse_Mount{
			Mount: &proto.MountResponse{
				Cookie: s.GetCookie(n),
				Claim:  proto.ClaimStatus_CLAIM_STATUS_EXCLUSIVE_WRITE_GRANTED,
				Stat:   StatProtoFromSysStat(&stat),
			},
		},
	}, nil
}

func (n *ServerNode) SetAttr(s *session, req *proto.SetAttrRequest) (*proto.OperationResponse, error) {
	if req.Stat == nil {
		return nil, unix.EINVAL
	}

	var updateFd int

	if req.Stat.Valid&(FATTR_SIZE|FATTR_MODE|FATTR_UID|FATTR_GID|FATTR_ATIME|FATTR_MTIME) != 0 {
		var err error
		updateFd, err = unix.Open(n.fdPath(), unix.O_RDWR, 0)
		if err != nil {
			return nil, err
		}
		defer unix.Close(updateFd)
	}

	if req.Stat.Valid&FATTR_MODE != 0 {
		if err := unix.Fchmod(updateFd, req.Stat.Mode); err != nil {
			return nil, err
		}
	}
	if req.Stat.Valid&FATTR_UID != 0 || req.Stat.Valid&FATTR_GID != 0 {
		uid, gid := -1, -1
		if req.Stat.Valid&FATTR_UID != 0 {
			uid = int(req.Stat.Uid)
		}
		if req.Stat.Valid&FATTR_GID != 0 {
			gid = int(req.Stat.Gid)
		}
		if err := unix.Fchown(updateFd, uid, gid); err != nil {
			return nil, err
		}
	}
	if req.Stat.Valid&FATTR_SIZE != 0 {
		// TODO: Needs a writable FD.
		if err := unix.Ftruncate(updateFd, int64(req.Stat.Size)); err != nil {
			return nil, err
		}
	}
	if req.Stat.Valid&FATTR_ATIME != 0 || req.Stat.Valid&FATTR_MTIME != 0 {
		var tv [2]unix.Timeval
		tv[0] = unix.NsecToTimeval(req.Stat.Atime.Seconds*1e9 + int64(req.Stat.Atime.Nanos))
		tv[1] = unix.NsecToTimeval(req.Stat.Mtime.Seconds*1e9 + int64(req.Stat.Mtime.Nanos))
		if err := unix.Futimes(updateFd, tv[:]); err != nil {
			return nil, err
		}
	}

	var stat unix.Stat_t
	if err := unix.Fstat(n.fd, &stat); err != nil {
		return nil, err
	}
	return &proto.OperationResponse{
		Response: &proto.OperationResponse_SetAttr{
			SetAttr: &proto.SetAttrResponse{
				Stat: StatProtoFromSysStat(&stat),
			},
		},
	}, nil
}

func (n *ServerNode) Mknod(s *session, req *proto.MknodRequest) (*proto.OperationResponse, error) {
	if err := unix.Mknodat(n.fd, req.Name, req.Stat.Mode, 0); err != nil {
		return nil, err
	}
	child, err := s.volume.GetNode(n.fd, req.Name)
	if err != nil {
		unix.Unlinkat(n.fd, req.Name, 0) // cleanup
		return nil, err
	}
	var stat unix.Stat_t
	if err := unix.Fstat(child.fd, &stat); err != nil {
		child.Close()
		unix.Unlinkat(n.fd, req.Name, 0) // cleanup
		return nil, err
	}
	// The stat from mknodat doesn't contain all the information, so we update it.
	req.Stat.Valid = FATTR_UID | FATTR_GID
	if _, err := child.SetAttr(s, &proto.SetAttrRequest{Stat: req.Stat}); err != nil {
		child.Close()
		unix.Unlinkat(n.fd, req.Name, 0) // cleanup
		return nil, err
	}
	if err := unix.Fstat(child.fd, &stat); err != nil {
		child.Close()
		unix.Unlinkat(n.fd, req.Name, 0) // cleanup
		return nil, err
	}

	return &proto.OperationResponse{
		Response: &proto.OperationResponse_Mknod{
			Mknod: &proto.MknodResponse{
				Cookie: s.GetCookie(child),
				Stat:   StatProtoFromSysStat(&stat),
			},
		},
	}, nil
}

func (n *ServerNode) Mkdir(s *session, req *proto.MkdirRequest) (*proto.OperationResponse, error) {
	err := unix.Mkdirat(n.fd, req.Name, req.Stat.Mode)
	if err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}
	newNode, err := n.volume.GetNode(n.fd, req.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to get new node: %w", err)
	}
	var stat unix.Stat_t
	if err := unix.Fstat(newNode.fd, &stat); err != nil {
		newNode.Close()
		unix.Unlinkat(n.fd, req.Name, unix.AT_REMOVEDIR) // cleanup
		return nil, fmt.Errorf("failed to stat new node: %w", err)
	}
	return &proto.OperationResponse{
		Response: &proto.OperationResponse_Mkdir{
			Mkdir: &proto.MkdirResponse{
				Cookie: s.GetCookie(newNode),
				Stat:   StatProtoFromSysStat(&stat),
			},
		},
	}, nil
}

func (n *ServerNode) Rmdir(s *session, req *proto.RmdirRequest) (*proto.OperationResponse, error) {
	err := unix.Unlinkat(n.fd, req.Name, unix.AT_REMOVEDIR)
	if err != nil {
		debugf("rmdir failed: %v", err)
		return nil, err
	}
	return &proto.OperationResponse{
		Response: &proto.OperationResponse_Rmdir{
			Rmdir: &proto.RmdirResponse{},
		},
	}, nil
}

func (n *ServerNode) Unlink(s *session, req *proto.UnlinkRequest) (*proto.OperationResponse, error) {
	err := unix.Unlinkat(n.fd, req.Name, 0)
	if err != nil {
		debugf("unlink failed: %v", err)
		return nil, err
	}
	return &proto.OperationResponse{
		Response: &proto.OperationResponse_Unlink{
			Unlink: &proto.UnlinkResponse{},
		},
	}, nil
}

func (n *ServerNode) Symlink(s *session, req *proto.SymlinkRequest) (*proto.OperationResponse, error) {
	err := unix.Symlinkat(req.Target, n.fd, req.Name)
	if err != nil {
		debugf("symlink failed: %v", err)
		return nil, err
	}
	var stat unix.Stat_t
	if err := unix.Fstatat(n.fd, req.Name, &stat, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		debugf("fstatat failed: %v", err)
		unix.Unlinkat(n.fd, req.Name, 0) // cleanup
		return nil, err
	}
	// Symlinks don't have persistent fds, so we create a node without one.
	child, err := s.volume.GetNode(n.fd, req.Name)
	if err != nil {
		unix.Unlinkat(n.fd, req.Name, 0) // cleanup
		return nil, err
	}
	cookie := s.GetCookie(child)
	return &proto.OperationResponse{
		Response: &proto.OperationResponse_Symlink{
			Symlink: &proto.SymlinkResponse{
				Cookie: cookie,
				Stat:   StatProtoFromSysStat(&stat),
			},
		},
	}, nil
}

func (n *ServerNode) Readlink(s *session, req *proto.ReadlinkRequest) (*proto.OperationResponse, error) {
	// There is no Readlinkat in linux, so we need to construct the path.
	// This is inherently racy.
	buf := make([]byte, unix.PathMax)
	count, err := unix.Readlinkat(n.fd, "", buf)
	if err != nil {
		debugf("readlink failed: %v", err)
		return nil, err
	}
	target := buf[:count]
	return &proto.OperationResponse{
		Response: &proto.OperationResponse_Readlink{
			Readlink: &proto.ReadlinkResponse{
				Target: target,
			},
		},
	}, nil
}

func (n *ServerNode) ScanDirectory(s *session, req *proto.ScanDirectoryRequest) (*proto.OperationResponse, error) {
	// This is a simplified implementation that reads all entries at once.
	// A proper implementation would handle the token for pagination.
	df, err := unix.Open(n.fdPath(), unix.O_RDONLY|unix.O_DIRECTORY, 0)
	if err != nil {
		return nil, err
	}
	defer unix.Close(df)

	// 4096 is a reasonable buffer size for directory entries
	buf := make([]byte, 4096)
	nread, err := unix.Getdents(df, buf)
	if err != nil {
		return nil, err
	}

	var entries []*proto.DirEntry
	for ptr := 0; ptr < nread; {
		dirent := (*unix.Dirent)(unsafe.Pointer(&buf[ptr]))
		nameBytes := buf[ptr+int(unsafe.Offsetof(unix.Dirent{}.Name)) : ptr+int(dirent.Reclen)]
		name := string(bytes.TrimRight(nameBytes, "\x00"))

		if name != "." && name != ".." {
			var stat unix.Stat_t
			if err := unix.Fstatat(n.fd, name, &stat, unix.AT_SYMLINK_NOFOLLOW); err == nil {
				child, err := s.volume.GetNode(n.fd, name)
				if err != nil {
					continue
				}
				entries = append(entries, &proto.DirEntry{
					Name:   name,
					Cookie: s.GetCookie(child),
					Inode:  stat.Ino,
					Type:   uint32(dirent.Type),
				})
			}
		}
		ptr += int(dirent.Reclen)
	}

	return &proto.OperationResponse{
		Response: &proto.OperationResponse_ScanDir{
			ScanDir: &proto.ScanDirectoryResponse{
				Entries: entries,
			},
		},
	}, nil
}

func (n *ServerNode) Create(s *session, req *proto.CreateRequest) (*proto.OperationResponse, error) {
	fd, err := unix.Openat(n.fd, req.Name, int(req.Flags), req.Stat.Mode)
	if err != nil {
		return nil, err
	}
	child, err := s.volume.GetNode(n.fd, req.Name)
	if err != nil {
		unix.Close(fd)
		unix.Unlinkat(n.fd, req.Name, 0)
		return nil, err
	}
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		unix.Close(fd)
		child.Close()
		unix.Unlinkat(n.fd, req.Name, 0)
		return nil, err
	}
	fh := s.newFileHandle(fd)
	return &proto.OperationResponse{
		Response: &proto.OperationResponse_Create{
			Create: &proto.CreateResponse{
				Cookie:     s.GetCookie(child),
				FileHandle: fh,
				Stat:       StatProtoFromSysStat(&stat),
			},
		},
	}, nil
}

func (n *ServerNode) Open(s *session, req *proto.OpenRequest) (*proto.OperationResponse, error) {
	fd, err := unix.Open(n.fdPath(), int(req.Flags), 0)
	if err != nil {
		return nil, err
	}
	fh := s.newFileHandle(fd)
	return &proto.OperationResponse{
		Response: &proto.OperationResponse_Open{
			Open: &proto.OpenResponse{
				FileHandle: fh,
			},
		},
	}, nil
}

func (n *ServerNode) Read(s *session, req *proto.ReadRequest) (*proto.OperationResponse, error) {
	fd, ok := s.getFd(req.FileHandle)
	if !ok {
		return nil, unix.EBADF
	}
	buf := make([]byte, req.Size)
	count, err := unix.Pread(fd, buf, req.Offset)
	if err != nil {
		return nil, err
	}
	return &proto.OperationResponse{
		Response: &proto.OperationResponse_Read{
			Read: &proto.ReadResponse{
				Data: buf[:count],
			},
		},
	}, nil
}

func (n *ServerNode) Write(s *session, req *proto.WriteRequest) (*proto.OperationResponse, error) {
	fd, ok := s.getFd(req.FileHandle)
	if !ok {
		return nil, unix.EBADF
	}
	count, err := unix.Pwrite(fd, req.Data, req.Offset)
	if err != nil {
		return nil, err
	}
	return &proto.OperationResponse{
		Response: &proto.OperationResponse_Write{
			Write: &proto.WriteResponse{
				Size: uint32(count),
			},
		},
	}, nil
}

func (n *ServerNode) CloseOp(s *session, req *proto.CloseRequest) (*proto.OperationResponse, error) {
	s.closeFileHandle(req.FileHandle)
	return &proto.OperationResponse{
		Response: &proto.OperationResponse_Close{
			Close: &proto.CloseResponse{},
		},
	}, nil
}

func (n *ServerNode) fdPath() string {
	// This is inherently racy, but we need to construct the path for operations like Readlink.
	return fmt.Sprintf("/proc/self/fd/%d", n.fd)
}

func (n *ServerNode) NotifyRevokeWriter(s *session) {
	s.SendNotify(n, &proto.OperationResponse{
		ServerRequest: &proto.OperationResponse_ClaimUpdate{
			ClaimUpdate: &proto.ClaimUpdateServerRequest{
				Status: proto.ClaimStatus_CLAIM_STATUS_EXCLUSIVE_WRITE_REVOKED,
			},
		},
	}, func(req *proto.OperationRequest, err error) {
		if err != nil {
			debugf("Failed to notify revoke writer: %v", err)
			return
		}
		n.tracker.RevokedWriter(s)
	})
}

func (n *ServerNode) NotifyRevokeReader(s *session) {
	s.SendNotify(n, &proto.OperationResponse{
		ServerRequest: &proto.OperationResponse_ClaimUpdate{
			ClaimUpdate: &proto.ClaimUpdateServerRequest{
				Status: proto.ClaimStatus_CLAIM_STATUS_LOCK_READ_REVOKED,
			},
		},
	}, func(req *proto.OperationRequest, err error) {
		if err != nil {
			debugf("Failed to notify revoke reader: %v", err)
			return
		}
		n.tracker.RevokedReader(s)
	})
}

func (n *ServerNode) ClaimWriter(s *session) bool {
	return false
}

func (n *ServerNode) ClaimReader(s *session) bool {
	return false
}
