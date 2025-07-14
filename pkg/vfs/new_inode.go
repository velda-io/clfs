package vfs

import (
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"velda.io/mtfs/pkg/proto"
)

type InodeInterface interface {
	fs.InodeEmbedder
	ResolveCookie(cookie []byte)
	handleClaimUpdate(proto.ClaimStatus)
}

func NewInode(serverProtocol ServerProtocol, cookie []byte, initialSyncGrants int, initialStat *proto.FileStat) InodeInterface {
	flags := initialStat.Mode
	if flags&syscall.S_IFDIR != 0 {
		return NewDirInode(serverProtocol, cookie, initialSyncGrants, initialStat)
	} else if flags&syscall.S_IFLNK != 0 {
		return NewLinkNode(serverProtocol, cookie, 0, nil, initialStat)
	} else if flags&syscall.S_IFREG != 0 {
		return NewFileInode(serverProtocol, cookie, initialSyncGrants, initialStat)
	}
	panic("Unsupported inode type")
}
