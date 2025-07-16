package vfs

import (
	"context"
	"log"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	"google.golang.org/protobuf/types/known/timestamppb"
	"velda.io/clfs/pkg/proto"
)

func StatProtoFromAttr(attr *fuse.Attr) *proto.FileStat {
	stat := &proto.FileStat{
		Mode: uint32(attr.Mode),
		Uid:  attr.Uid,
		Gid:  attr.Gid,
		Size: attr.Size,
	}
	stat.Atime = timestamppb.New(attr.AccessTime())
	stat.Mtime = timestamppb.New(attr.ModTime())
	stat.Ctime = timestamppb.New(attr.ChangeTime())
	return stat
}

func AttrFromStatProto(stat *proto.FileStat) *fuse.Attr {
	const blockSize = 4096 // Default block size
	attr := &fuse.Attr{
		Mode:    stat.Mode,
		Owner:   fuse.Owner{Uid: stat.Uid, Gid: stat.Gid},
		Size:    stat.Size,
		Nlink:   1,                                       // Default to 1 link
		Blksize: blockSize,                               // Default block size
		Blocks:  (stat.Size + blockSize - 1) / blockSize, // Calculate blocks based on size
	}
	var atime, mtime, ctime *time.Time
	if stat.Atime != nil {
		time := stat.Atime.AsTime()
		atime = &time
	}
	if stat.Mtime != nil {
		time := stat.Mtime.AsTime()
		mtime = &time
	}
	if stat.Ctime != nil {
		time := stat.Ctime.AsTime()
		ctime = &time
	}
	attr.SetTimes(atime, mtime, ctime)
	return attr
}

func emptyFileStatProto(context context.Context, mode uint32) *proto.FileStat {
	caller, ok := fuse.FromContext(context)
	if !ok {
		caller = &fuse.Caller{}
	}
	return &proto.FileStat{
		Valid: fuse.FATTR_MODE | fuse.FATTR_UID | fuse.FATTR_GID | fuse.FATTR_SIZE,
		Mode:  mode,
		Uid:   caller.Uid,
		Gid:   caller.Gid,
		Size:  0,
	}
}

func newFileStat(context context.Context, out *fuse.Attr, mode uint32) {
	out.Mode = mode
	out.Nlink = 1      // Default to 1 link
	out.Blksize = 4096 // Default block size
	out.Blocks = 0     // Default to 0 blocks
	caller, ok := fuse.FromContext(context)
	if !ok {
		caller = &fuse.Caller{}
	}
	out.Owner.Uid = caller.Uid
	out.Owner.Gid = caller.Gid
	now := time.Now()
	out.SetTimes(&now, &now, &now)
}

func DefaultRootStat() *proto.FileStat {
	return &proto.FileStat{
		Mode: syscall.S_IFDIR | 0755, // Default root directory permissions
		Uid:  0,                      // Default UID
		Gid:  0,                      // Default GID
		Size: 4096,                   // Default size for root directory
	}
}

var debug bool

func SetDebug(d bool) {
	debug = d
}

func debugf(format string, args ...interface{}) {
	if debug {
		log.Printf(format, args...)
	}
}
