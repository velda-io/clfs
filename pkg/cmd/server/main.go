package main

import (
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"

	"velda.io/mtfs/pkg/proto"
	"velda.io/mtfs/pkg/server"
)

func main() {
	// Define flags for endpoint and root directory
	endpoint := flag.String("endpoint", "localhost:50055", "The endpoint to listen on")
	rootDir := flag.String("root", "./", "The root directory for the service")
	flag.Parse()

	// Start the gRPC server
	listener, err := net.Listen("tcp", *endpoint)
	if err != nil {
		log.Fatalf("Failed to listen on %s: %v", *endpoint, err)
	}

	grpcServer := grpc.NewServer()
	service := server.NewMtfsServiceServer()
	proto.RegisterMtfsServiceServer(grpcServer, service)

	// Handle termination signals
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)

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
