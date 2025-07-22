package vfs

import (
	"context"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"velda.io/clfs/pkg/proto"
)

type LinkNode struct {
	Inode
	Target []byte // The target path of the symlink
}

func NewLinkNode(svc ServerProtocol, cookie []byte, flags int, target []byte, stat *proto.FileStat) *LinkNode {
	node := &LinkNode{
		Target: target,
	}
	node.init(svc, cookie, flags, stat, node)
	return node
}

var _ = (fs.NodeReadlinker)((*LinkNode)(nil))

func (n *LinkNode) Readlink(ctx context.Context) ([]byte, syscall.Errno) {
	op := n.syncer.StartRead()
	defer n.syncer.Complete(op)
	if op.Async() {
		return n.Target, fs.OK
	}
	response, err := n.syncOperation(ctx, &proto.OperationRequest{
		Operation: &proto.OperationRequest_Readlink{
			Readlink: &proto.ReadlinkRequest{},
		},
	})
	if err != nil {
		return nil, fs.ToErrno(err)
	}
	switch resp := response.Response.(type) {
	case *proto.OperationResponse_Readlink:
		if resp.Readlink == nil {
			return nil, syscall.EIO
		}
		n.Target = resp.Readlink.Target
		return resp.Readlink.Target, fs.OK
	default:
		debugf("ReadLink: Unexpected response type: %T", resp)
		return nil, syscall.EIO
	}
}
