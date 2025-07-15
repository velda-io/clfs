package server

import (
	"fmt"
	"os"
	"sync"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fuse"
	"golang.org/x/sys/unix"
	"velda.io/mtfs/pkg/proto"
)

type ServerNode struct {
	volume *Volume
	mu     sync.Mutex
	fd     int
	nodeId int64
	mode   uint32
	stat   *proto.FileStat

	writerSession  *session
	readerSessions []*session
}

func (n *ServerNode) HandleOperation(s *session, op *proto.OperationRequest) (*proto.OperationResponse, error) {
	switch req := op.Operation.(type) {
	case *proto.OperationRequest_Mkdir:
		return n.Mkdir(s, req.Mkdir)
	case *proto.OperationRequest_Lookup:
		return n.Lookup(s, req.Lookup)
	case *proto.OperationRequest_Read:
		// Handle read operation
		return &proto.OperationResponse{
			Response: &proto.OperationResponse_Read{
				Read: &proto.ReadResponse{
					Data: []byte("file data example"),
				},
			},
		}, nil
	case *proto.OperationRequest_Mount:
		return &proto.OperationResponse{
			Response: &proto.OperationResponse_Mount{
				Mount: &proto.MountResponse{
					Cookie: s.GetCookie(n),
					Claim:  proto.ClaimStatus_CLAIM_STATUS_EXCLUSIVE_WRITE_GRANTED,
					Stat:   n.getAttr(),
				},
			},
		}, nil
	default:
		return nil, syscall.ENOTSUP
	}
}

func (n *ServerNode) Lookup(s *session, req *proto.LookupRequest) (*proto.OperationResponse, error) {
	newNode, err := n.volume.GetNode(n.fd, req.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to lookup node: %w", err)
	}
	return &proto.OperationResponse{
		Response: &proto.OperationResponse_Lookup{
			Lookup: &proto.LookupResponse{
				Cookie: s.GetCookie(newNode),
				Stat:   newNode.getAttr(),
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
	newNode.setAttr(req.Stat)
	return &proto.OperationResponse{
		Response: &proto.OperationResponse_Mkdir{
			Mkdir: &proto.MkdirResponse{
				Cookie: s.GetCookie(n),
				Stat:   newNode.getAttr(),
			},
		},
	}, nil
}

func (n *ServerNode) setAttr(attr *proto.FileStat) {
	if attr == nil {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if attr.Valid&fuse.FATTR_MODE != 0 {
		// Ignore type.
		mode := attr.Mode&uint32(os.ModePerm) | n.mode&uint32(os.ModeType)
		n.mode = mode
		unix.Fchmod(n.fd, mode)
	}
	changedOwner := false
	if attr.Valid&fuse.FATTR_UID != 0 {
		changedOwner = changedOwner || n.stat.Uid != attr.Uid
		n.stat.Uid = attr.Uid
	}
	if attr.Valid&fuse.FATTR_GID != 0 {
		changedOwner = changedOwner || n.stat.Gid != attr.Gid
		n.stat.Gid = attr.Gid
	}
	if changedOwner {
		unix.Fchown(n.fd, int(attr.Uid), int(attr.Gid))
	}
	if attr.Valid&fuse.FATTR_SIZE != 0 {
		n.stat.Size = attr.Size
		// TODO: Handle size change
	}
	changedTime := false
	if attr.Valid&fuse.FATTR_ATIME != 0 {
		n.stat.Atime = attr.Atime
		changedTime = true
	}
	if attr.Valid&fuse.FATTR_MTIME != 0 {
		n.stat.Mtime = attr.Mtime
		changedTime = true
	}
	if changedTime {
		unix.Futimes(n.fd, []unix.Timeval{
			{
				Sec:  n.stat.Atime.Seconds,
				Usec: int64(n.stat.Atime.Nanos / 1000),
			}, {
				Sec:  n.stat.Mtime.Seconds,
				Usec: int64(n.stat.Mtime.Nanos / 1000),
			}})
	}
}

func (n *ServerNode) getAttr() *proto.FileStat {
	return n.stat
}
