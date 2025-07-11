package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

type MountOptions func(*fs.Options)

func MountWorkDir(baseDir, workspaceDir string, options ...MountOptions) (*fuse.Server, error) {
	root, err := fs.NewLoopbackRoot(baseDir)
	if err != nil {
		return nil, err
	}
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
	if len(os.Args) < 3 {
		log.Fatalf("Usage: %s <base-dir> <workspace-dir>", os.Args[0])
	}
	baseDir := os.Args[1]
	workspaceDir := os.Args[2]

	server, err := MountWorkDir(baseDir, workspaceDir)
	if err != nil {
		log.Fatalf("Failed to mount: %v", err)
	}
	log.Printf("Mounted %s at %s", baseDir, workspaceDir)
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
