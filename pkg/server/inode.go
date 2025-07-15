package server

import (
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/hanwen/go-fuse/v2/fuse"
	"velda.io/mtfs/pkg/proto"
)

type session struct {
}

type serviceCoordinator interface {
	GetNode(path string) (*ServerNode, error)
	GetCookie(s *session, node *ServerNode) []byte
}

type ServerNode struct {
	svc  serviceCoordinator
	mu   sync.Mutex
	path string
	mode uint32
	stat *proto.FileStat

	writerSession  *session
	readerSessions []*session
}

func (n *ServerNode) HandleOperation(s *session, op *proto.OperationRequest) (*proto.OperationResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	switch req := op.Operation.(type) {
	case *proto.OperationRequest_Mkdir:
		return n.Mkdir(s, req.Mkdir)
	case *proto.OperationRequest_Read:
		// Handle read operation
		return &proto.OperationResponse{
			Response: &proto.OperationResponse_Read{
				Read: &proto.ReadResponse{
					Data: []byte("file data example"),
				},
			},
		}, nil
	default:
		return nil, errors.New("unsupported operation")
	}
}

func (n *ServerNode) Mkdir(s *session, req *proto.MkdirRequest) (*proto.OperationResponse, error) {
	err := os.Mkdir(n.path+"/"+req.Name, os.FileMode(req.Stat.Mode))
	if err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}
	newNode, err := n.svc.GetNode(n.path + "/" + req.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to get new node: %w", err)
	}
	newNode.setAttr(req.Stat)
	return &proto.OperationResponse{
		Response: &proto.OperationResponse_Mkdir{
			Mkdir: &proto.MkdirResponse{
				Cookie: n.svc.GetCookie(s, newNode),
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
	}
	if attr.Valid&fuse.FATTR_UID != 0 {
		n.stat.Uid = attr.Uid
	}
	if attr.Valid&fuse.FATTR_GID != 0 {
		n.stat.Gid = attr.Gid
	}
	if attr.Valid&fuse.FATTR_SIZE != 0 {
		n.stat.Size = attr.Size
	}
	if attr.Valid&fuse.FATTR_ATIME != 0 {
		n.stat.Atime = attr.Atime
	}
	if attr.Valid&fuse.FATTR_MTIME != 0 {
		n.stat.Mtime = attr.Mtime
	}
	if attr.Valid&fuse.FATTR_CTIME != 0 {
		n.stat.Ctime = attr.Ctime
	}
}

func (n *ServerNode) getAttr() *proto.FileStat {
	return n.stat
}
