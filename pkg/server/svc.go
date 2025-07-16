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
}

func NewMtfsServiceServer(volumes map[string]*Volume) *MtfsServiceServer {
	return &MtfsServiceServer{
		volumes: volumes,
	}
}

func (s *MtfsServiceServer) Run(nproc int) {
}

func (s *MtfsServiceServer) Shutdown() {
}

func (s *MtfsServiceServer) Serve(stream proto.MtfsService_ServeServer) (e error) {
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
