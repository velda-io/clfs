package vfs

import (
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"velda.io/clfs/pkg/proto"
)

type InodeInterface interface {
	fs.InodeEmbedder
	ResolveCookie(cookie []byte)
	handleClaimUpdate(proto.ClaimStatus)
	ReceiveServerRequest(res *proto.OperationResponse)
	inode() *Inode
}

func NewInode(serverProtocol ServerProtocol, cookie []byte, initialSyncGrants int, initialStat *proto.FileStat) InodeInterface {
	flags := initialStat.Mode
	switch flags & syscall.S_IFMT {
	case syscall.S_IFDIR:
		return NewDirInode(serverProtocol, cookie, initialSyncGrants, initialStat)
	case syscall.S_IFLNK:
		return NewLinkNode(serverProtocol, cookie, 0, nil, initialStat)
	case syscall.S_IFREG:
		return NewFileInode(serverProtocol, cookie, initialSyncGrants, initialStat)
	}
	panic("Unsupported inode type")
}
