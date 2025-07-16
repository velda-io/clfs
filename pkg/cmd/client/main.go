package main

import (
	"context"
	"flag"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"velda.io/mtfs/pkg/vfs"
)

var debug = flag.Bool("debug", false, "Enable debug logging")
var endpoint = flag.String("endpoint", "dns:///localhost:50055", "gRPC endpoint to connect to")

type MountOptions func(*fs.Options)

func RunClient(endpoint string) vfs.ServerProtocol {
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to endpoint %s: %v", endpoint, err)
	}

	c := vfs.NewClient(conn)
	err = c.Start(context.Background())
	if err != nil {
		log.Fatalf("Client start error: %v", err)
	}
	go func() {
		err := c.Run(context.Background())
		if err != nil {
			log.Printf("Client run error: %v", err)
		}
	}()

	log.Printf("gRPC connection established with endpoint %s", endpoint)
	return c
}

func Mount(volume, workspaceDir string, options ...MountOptions) (*fuse.Server, error) {
	svc := RunClient(*endpoint)

	root := vfs.NewDirInode(svc, nil, 0, vfs.DefaultRootStat())
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
		OnAdd: func(ctx context.Context) {
			err := root.Mount(ctx, volume)
			if err != nil {
				log.Fatalf("Failed to mount volume %s: %v", volume, err)
			}
			log.Printf("Mounted volume %s at %s", volume, workspaceDir)
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
	if flag.NArg() != 2 {
		log.Fatalf("Usage: %s <volume> <dir>", os.Args[0])
	}

	go func() {
		lis, _ := net.Listen("tcp", ":6070")
		http.Serve(lis, nil)
	}()

	volume := flag.Arg(0)
	workspaceDir := flag.Arg(1)

	server, err := Mount(volume, workspaceDir)
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
