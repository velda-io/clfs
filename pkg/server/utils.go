package server

import (
	"errors"
	"log"
	"syscall"

	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/types/known/timestamppb"
	"velda.io/mtfs/pkg/proto"
)

func StatProtoFromSysStat(stat *unix.Stat_t) *proto.FileStat {
	result := &proto.FileStat{
		Inode: stat.Ino,
		Mode:  stat.Mode,
		Nlink: stat.Nlink,
		Uid:   uint32(stat.Uid),
		Gid:   uint32(stat.Gid),
		Size:  uint64(stat.Size),
	}
	// Add timestamps
	result.Atime = &timestamppb.Timestamp{
		Seconds: stat.Atim.Sec,
		Nanos:   int32(stat.Atim.Nsec),
	}
	result.Mtime = &timestamppb.Timestamp{
		Seconds: stat.Mtim.Sec,
		Nanos:   int32(stat.Mtim.Nsec),
	}
	result.Ctime = &timestamppb.Timestamp{
		Seconds: stat.Ctim.Sec,
		Nanos:   int32(stat.Ctim.Nsec),
	}
	return result
}

func ToErrno(err error) syscall.Errno {
	var errno syscall.Errno
	if errors.As(err, &errno) {
		return errno
	}
	debugf("can't convert error type: %v", err)
	return syscall.ENOSYS
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
