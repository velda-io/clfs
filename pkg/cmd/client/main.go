package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"velda.io/mtfs/pkg/client"
	"velda.io/mtfs/pkg/vfs"
)

var debug = flag.Bool("debug", false, "Enable debug logging")
var endpoint = flag.String("endpoint", "dns://localhost:50055", "gRPC endpoint to connect to")

type MountOptions func(*fs.Options)

func RunClient(endpoint string) vfs.ServerProtocol {
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to endpoint %s: %v", endpoint, err)
	}
	defer conn.Close()

	c := client.NewClient(conn)
	go func() {
		err := c.Run(context.Background())
		if err != nil {
			log.Fatalf("Client run error: %v", err)
		}
	}()
	log.Printf("gRPC connection established with endpoint %s", endpoint)
	return c
}

func MountWorkDir(workspaceDir string, options ...MountOptions) (*fuse.Server, error) {
	svc := RunClient(*endpoint)

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
