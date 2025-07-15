package server

import (
	"log"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"velda.io/mtfs/pkg/proto"
)

type MtfsServiceServer struct {
	proto.UnimplementedMtfsServiceServer

	volumes map[string]*Volume
	ops     chan func()
}

func NewMtfsServiceServer(volumes map[string]*Volume) *MtfsServiceServer {
	return &MtfsServiceServer{
		volumes: volumes,
		ops:     make(chan func(), 100), // Buffered channel for operations
	}
}

func (s *MtfsServiceServer) Run(nproc int) {
	for i := 0; i < nproc; i++ {
		go func() {
			for op := range s.ops {
				op()
			}
		}()
	}
}

func (s *MtfsServiceServer) Shutdown() {
	close(s.ops) // Close the operations channel to stop all goroutines
}

func (s *MtfsServiceServer) Serve(stream proto.MtfsService_ServeServer) (e error) {
	defer func() {
		if e != nil {
			log.Printf("Exiting with err %v", e)
		}
	}()
	log.Printf("Connected")
	req, err := stream.Recv()
	if err != nil {
		return err
	}
	mnt := req.GetMount()
	if mnt == nil {
		return status.Error(codes.InvalidArgument, "First request must be a mount request")
	}
	volume := s.volumes[mnt.Volume]
	if volume == nil {
		return status.Errorf(codes.NotFound, "Volume %s not found", mnt.Volume)
	}
	sess := &session{stream: stream, volume: volume}
	sess.handleOp(volume.root, req)
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}
		s.ops <- func() {
			sess.HandleOp(req)
		}
	}
}
