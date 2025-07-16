package main

import (
	"flag"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"

	"velda.io/clfs/pkg/proto"
	"velda.io/clfs/pkg/server"
)

func main() {
	// Define flags for endpoint and root directory
	endpoint := flag.String("endpoint", "localhost:50055", "The endpoint to listen on")
	rootDir := flag.String("root", "./", "The root directory for the service")
	volumeName := flag.String("vname", "volume", "Name of the volume")
	debug := flag.Bool("debug", false, "Enable debug logging")
	flag.Parse()

	go func() {
		lis, _ := net.Listen("tcp", ":6071")
		http.Serve(lis, nil)
	}()
	server.SetDebug(*debug)
	// Start the gRPC server
	listener, err := net.Listen("tcp", *endpoint)
	if err != nil {
		log.Fatalf("Failed to listen on %s: %v", *endpoint, err)
	}

	volume, err := server.NewVolume(*rootDir)
	if err != nil {
		log.Fatalf("Failed to open volume %s: %v", *rootDir, err)
	}

	volumes := make(map[string]*server.Volume)
	volumes[*volumeName] = volume

	grpcServer := grpc.NewServer()
	service := server.NewClfsServiceServer(volumes)
	proto.RegisterClfsServiceServer(grpcServer, service)

	// Handle termination signals
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)

	service.Run(10)
	go func() {
		log.Printf("Starting server at %s with root directory %s", *endpoint, *rootDir)
		if err := grpcServer.Serve(listener); err != nil {
			log.Fatalf("Failed to serve: %v", err)
		}
	}()

	<-stop
	log.Println("Shutting down server...")
	grpcServer.GracefulStop()
	log.Println("Server stopped.")
}
