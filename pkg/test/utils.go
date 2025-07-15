package test

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"velda.io/mtfs/pkg/client"
	"velda.io/mtfs/pkg/proto"
	"velda.io/mtfs/pkg/server"
	"velda.io/mtfs/pkg/vfs"
)

type TestServer struct {
	Addr string
	svc  *server.MtfsServiceServer
}

func StartTestServer(t *testing.T) *TestServer {
	path := t.TempDir()
	return StartTestServerWithPath(t, path)
}

func StartTestServerWithPath(t *testing.T, path string) *TestServer {

	// Start the gRPC server
	listener, err := net.Listen("tcp", ":")

	addr := listener.Addr().String()

	volume, err := server.NewVolume(path)
	if err != nil {
		t.Fatalf("Failed to create volume: %v", err)
	}

	volumes := make(map[string]*server.Volume)
	volumes["volume"] = volume

	grpcServer := grpc.NewServer()
	service := server.NewMtfsServiceServer(volumes)
	proto.RegisterMtfsServiceServer(grpcServer, service)

	// Handle termination signals
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)

	service.Run(10)
	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			t.Fatal("Failed to serve:", err)
		}
	}()
	t.Cleanup(func() {
		service.Shutdown()
		grpcServer.Stop()
		listener.Close()
	})
	return &TestServer{
		Addr: addr,
		svc:  service,
	}
}

type TestClient struct {
	svc *fuse.Server
}

func runClient(t *testing.T, endpoint string) vfs.ServerProtocol {
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	assert.NoError(t, err, "Failed to connect to endpoint %s", endpoint)

	c := client.NewClient(conn)
	err = c.Start(context.Background())
	assert.NoError(t, err, "Client start error")
	go func() {
		err := c.Run(context.Background())
		// TODO: Assert this.
		log.Printf("Client run finished with error: %v", err)
	}()

	log.Printf("gRPC connection established with endpoint %s", endpoint)
	return c
}

func Mount(t *testing.T, server *TestServer, mode int) (string, *fuse.Server) {
	t.Helper()
	client := runClient(t, "dns:///"+server.Addr)

	mntDir := t.TempDir()

	root := vfs.NewDirInode(client, nil, mode, vfs.DefaultRootStat())
	timeout := 60 * time.Second
	negativeTimeout := 10 * time.Second
	option := &fs.Options{
		EntryTimeout:    &timeout,
		AttrTimeout:     &timeout,
		NegativeTimeout: &negativeTimeout,
		MountOptions: fuse.MountOptions{
			//AllowOther:         true,
			DisableReadDirPlus: true,
			Name:               "mtfs",
			MaxWrite:           1024 * 1024,
			EnableLocks:        true,
			Debug:              testing.Verbose(),
			//DirectMountFlags:   syscall.MS_MGC_VAL,
		},
		OnAdd: func(ctx context.Context) {
			assert.NoError(t, root.Mount(ctx, "volume"), "Failed to mount")

		},
	}
	mnt, err := fs.Mount(mntDir, root, option)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := mnt.Unmount(); err != nil {
			t.Fatalf("testMount: Unmount failed: %v", err)
		}
	})
	return mntDir, mnt
}
