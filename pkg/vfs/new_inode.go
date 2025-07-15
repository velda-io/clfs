package vfs

import (
	"log"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"velda.io/mtfs/pkg/proto"
)

type InodeInterface interface {
	ResolveCookie(cookie []byte)
	handleClaimUpdate(proto.ClaimStatus)
}

func NewInode(serverProtocol ServerProtocol, cookie []byte, initialSyncGrants int, initialStat *proto.FileStat) fs.InodeEmbedder {
	flags := initialStat.Mode
	log.Printf("Creating INode for mode %d", flags)
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
