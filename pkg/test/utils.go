package test

import (
	"context"
	"log"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"velda.io/mtfs/pkg/proto"
	"velda.io/mtfs/pkg/server"
	"velda.io/mtfs/pkg/vfs"
)

type TestServer struct {
	Addr string
	svc  *server.MtfsServiceServer
}

func StartTestServer(t *testing.T) *TestServer {
	//path, _ := os.MkdirTemp("", "mtfs-test-server")
	path := t.TempDir()
	t.Log("Test server path:", path)
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
			log.Printf("Failed to run gRPC server: %v", err)
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

func runClient(endpoint string, latency time.Duration) *vfs.Client {
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to endpoint %s: %v", endpoint, err)
	}

	c := vfs.NewClient(conn)
	err = c.Start(context.Background())
	if err != nil {
		log.Fatalf("Failed to start client: %v", err)
	}
	if latency > 0 {
		c.Stream = NewLatencyInjectedStream(c.Stream, latency)
	}
	go func() {
		err := c.Run(context.Background())
		// TODO: Assert this.
		log.Printf("Client run finished with error: %v", err)
	}()

	return c
}

func doMount(addr, dir string, debug bool, mode int, latency time.Duration) {
	client := runClient("dns:///"+addr, latency)
	root := vfs.NewDirInode(client, nil, mode, vfs.DefaultRootStat())
	timeout := 60 * time.Second
	negativeTimeout := 10 * time.Second
	vfs.SetDebug(debug)
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
			Debug:              debug,
			//DirectMountFlags:   syscall.MS_MGC_VAL,
		},
		OnAdd: func(ctx context.Context) {
			err := root.Mount(ctx, "volume")
			if err != nil {
				log.Fatal(err, "Failed to mount")
			}
		},
	}
	mnt, err := fs.Mount(dir, root, option)
	if err != nil {
		log.Fatal(err)
	}
	// Kill by SIGTERM
	sig := make(chan os.Signal, 1)
	stopped := false
	go func() {
		mnt.Wait()
		stopped = true
		signal.Stop(sig)
		close(sig)
	}()
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
	<-sig
	signal.Stop(sig)
	if !stopped {
		if err := mnt.Unmount(); err != nil {
			log.Fatalf("testMount: Unmount failed: %v", err)
		}
	}
	client.Shutdown()
}

func mountMain() {
	addr := os.Getenv("MTFS_TEST_MOUNT_ADDR")
	dir := os.Getenv("MTFS_TEST_MOUNT_DIR")
	debug := os.Getenv("MTFS_TEST_MOUNT_DEBUG") == "1"
	mode, err := strconv.ParseInt(os.Getenv("MTFS_TEST_MOUNT_MODE"), 10, 32)
	latency, err := strconv.ParseInt(os.Getenv("MTFS_TEST_MOUNT_LATENCY"), 10, 32)
	if err != nil {
		log.Fatalf("Invalid MTFS_TEST_MOUNT_MODE: %v", err)
	}
	doMount(addr, dir, debug, int(mode), time.Duration(latency)*time.Millisecond)
	os.Exit(0)
}

func init() {
	if os.Getenv("MTFS_TEST_MOUNT") != "" {
		mountMain()
	}
}

func TestMain(m *testing.M) {
	server.SetDebug(testing.Verbose())
	code := m.Run()
	os.Exit(code)
}

func Mount(t *testing.T, server *TestServer, mode int, latency time.Duration) string {
	t.Helper()
	mntDir, stop := MountOne(t, server, mode, latency)
	t.Cleanup(stop)
	return mntDir
}

func MountOne(t *testing.T, server *TestServer, mode int, latency time.Duration) (string, func()) {
	t.Helper()

	mntDir := t.TempDir()
	oldStat, err := os.Stat(mntDir)
	if err != nil {
		t.Fatalf("Failed to stat mount directory %s: %v", mntDir, err)
	}
	// Use a subprocess to avoid dead-locks on errors.
	cmd := exec.Command(os.Args[0])
	cmd.Env = append(os.Environ(), "MTFS_TEST_MOUNT=1")
	cmd.Env = append(cmd.Env, "MTFS_TEST_MOUNT_ADDR="+server.Addr)
	cmd.Env = append(cmd.Env, "MTFS_TEST_MOUNT_DIR="+mntDir)
	cmd.Env = append(cmd.Env, "MTFS_TEST_MOUNT_MODE="+strconv.Itoa(mode))
	cmd.Env = append(cmd.Env, "MTFS_TEST_MOUNT_LATENCY="+strconv.Itoa(int(latency.Milliseconds())))
	if testing.Verbose() {
		cmd.Env = append(cmd.Env, "MTFS_TEST_MOUNT_DEBUG=1")
	}
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	stop := func() {
		cmd.Process.Signal(syscall.SIGTERM)
		cmd.Process.Wait()
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("Failed to start mount command: %v", err)
	}
	// Wait until mount is ready
	for {
		newStat, err := os.Stat(mntDir)
		if err != nil {
			t.Fatalf("Failed to stat mount directory %s: %v", mntDir, err)
		}
		if newStat.Sys().(*syscall.Stat_t).Dev != oldStat.Sys().(*syscall.Stat_t).Dev {
			// The mount point has changed, meaning the mount is ready.
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	return mntDir, stop
}
