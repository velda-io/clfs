package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"velda.io/mtfs/pkg/proto"
	"velda.io/mtfs/pkg/vfs"
)

var debug = flag.Bool("debug", false, "Enable debug logging")

type FakeSvc struct {
}

func (svc *FakeSvc) EnqueueOperation(request *proto.OperationRequest, callback vfs.OpCallback) int64 {
	// Fake implementation that just returns a success response
	go func() {
		callback(&proto.OperationResponse{}, nil)
	}()
	return 1
}

func (svc *FakeSvc) RegisterServerCallback(cookie []byte, callback vfs.ServerCallback) {
	// No-op implementation for fake service
}

func (svc *FakeSvc) UnregisterServerCallback(cookie []byte) {
	// No-op implementation for fake service
}

type MountOptions func(*fs.Options)

func MountWorkDir(workspaceDir string, options ...MountOptions) (*fuse.Server, error) {
	svc := &FakeSvc{}

	root := vfs.NewDirInode(svc, []byte("123"), vfs.SYNC_EXCLUSIVE_WRITE, vfs.DefaultRootStat())
	timeout := 60 * time.Second
	negativeTimeout := 10 * time.Second
	option := &fs.Options{
		EntryTimeout:    &timeout,
		AttrTimeout:     &timeout,
		NegativeTimeout: &negativeTimeout,
		MountOptions: fuse.MountOptions{
			AllowOther:         true,
			DisableReadDirPlus: true,
			DirectMountStrict:  true,
			Name:               "mtfs",
			MaxWrite:           1024 * 1024,
			EnableLocks:        true,
			DirectMountFlags:   syscall.MS_MGC_VAL,
			Debug:              *debug,
		},
	}
	for _, opt := range options {
		opt(option)
	}
	server, err := fs.Mount(workspaceDir, root, option)
	if err != nil {
		return nil, err
	}
	return server, nil
}

func main() {
	flag.Parse()
	if flag.NArg() != 1 {
		log.Fatalf("Usage: %s <workspace-dir>", os.Args[0])
	}
	workspaceDir := flag.Arg(0)

	server, err := MountWorkDir(workspaceDir)
	if err != nil {
		log.Fatalf("Failed to mount: %v", err)
	}
	log.Printf("Mounted at %s", workspaceDir)
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
	<-sig
	log.Printf("Unmounting %s", workspaceDir)
	for {
		err := server.Unmount()
		if err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

}
