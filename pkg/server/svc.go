package server

import (
	"velda.io/mtfs/pkg/proto"
)

type MtfsServiceServer struct {
	proto.UnimplementedMtfsServiceServer
}

func NewMtfsServiceServer() *MtfsServiceServer {
	return &MtfsServiceServer{}
}

func (s *MtfsServiceServer) Serve(stream proto.MtfsService_ServeServer) error {
	// Skeleton implementation for Serve RPC
	return nil
}
