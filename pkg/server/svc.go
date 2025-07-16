package server

import (
	"log"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"velda.io/clfs/pkg/proto"
)

type ClfsServiceServer struct {
	proto.UnimplementedClfsServiceServer

	volumes map[string]*Volume
}

func NewClfsServiceServer(volumes map[string]*Volume) *ClfsServiceServer {
	return &ClfsServiceServer{
		volumes: volumes,
	}
}

func (s *ClfsServiceServer) Run(nproc int) {
}

func (s *ClfsServiceServer) Shutdown() {
}

func (s *ClfsServiceServer) Serve(stream proto.ClfsService_ServeServer) (e error) {
	defer func() {
		if e != nil {
			log.Printf("Exiting with err %v", e)
		}
	}()
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
	sess := NewSession(stream, volume)
	sess.handleOp(volume.root, req)
	for {
		req, err := stream.Recv()
		if err != nil {
			sess.Close()
			return err
		}
		sess.HandleOp(req)
	}
}
