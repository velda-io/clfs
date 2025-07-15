package server

import (
	"bytes"
	"encoding/binary"
	"errors"
	"log"

	"golang.org/x/sys/unix"
	"velda.io/mtfs/pkg/proto"
)

type session struct {
	volume *Volume // The volume this session is associated with
	stream proto.MtfsService_ServeServer
}

func (s *session) GetCookie(node *ServerNode) []byte {
	buf := bytes.NewBuffer(nil)
	binary.Write(buf, binary.LittleEndian, uint64(node.nodeId))
	data := buf.Bytes()
	// TODO: Encrypt the data to avoid spoofing.
	return data
}

var ErrInvalidCookie = errors.New("invalid cookie")

func (s *session) DecodeCookie(cookie []byte) (*ServerNode, error) {
	if len(cookie) < 8 {
		return nil, ErrInvalidCookie
	}
	var nodeId uint64
	buf := bytes.NewBuffer(cookie)
	if err := binary.Read(buf, binary.LittleEndian, &nodeId); err != nil {
		return nil, ErrInvalidCookie
	}
	if buf.Len() != 0 {
		return nil, ErrInvalidCookie
	}
	node := s.volume.GetNodeById(int64(nodeId))
	if node == nil {
		return nil, ErrInvalidCookie
	}
	return node, nil
}

func (sess *session) HandleOp(req *proto.OperationRequest) {
	node, err := sess.DecodeCookie(req.Cookie)
	if err != nil {
		sess.stream.Send(&proto.OperationResponse{
			Error: &proto.ErrorResponse{
				DetailMsg: "Invalid cookie",
				Code:      int32(unix.EINVAL),
			},
		})
		return
	}
	sess.handleOp(node, req)
}

func (sess *session) handleOp(node *ServerNode, req *proto.OperationRequest) {
	log.Printf("Rx: %d", req.SeqId)
	resp, err := node.HandleOperation(sess, req)
	log.Printf("Tx: %d", req.SeqId)
	if err != nil {
		sess.stream.Send(&proto.OperationResponse{
			SeqId: req.SeqId,
			Error: &proto.ErrorResponse{
				DetailMsg: err.Error(),
				Code:      int32(ToErrno(err)),
			},
		})
		return
	}
	resp.SeqId = req.SeqId
	if err := sess.stream.Send(resp); err != nil {
		log.Printf("Stream failure: %v", err)
	}
}
