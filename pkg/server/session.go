package server

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"log"
	"sync"

	"golang.org/x/sys/unix"
	"velda.io/mtfs/pkg/proto"
)

type session struct {
	volume      *Volume // The volume this session is associated with
	stream      proto.MtfsService_ServeServer
	fileHandles map[string]int // map file handle to fd
	fhMu        sync.Mutex
}

func (s *session) newFileHandle(fd int) []byte {
	s.fhMu.Lock()
	defer s.fhMu.Unlock()
	if s.fileHandles == nil {
		s.fileHandles = make(map[string]int)
	}
	for {
		handle := make([]byte, 16)
		if _, err := rand.Read(handle); err != nil {
			panic(err) // Should not happen
		}
		handleStr := string(handle)
		if _, exists := s.fileHandles[handleStr]; !exists {
			s.fileHandles[handleStr] = fd
			return handle
		}
	}
}

func (s *session) getFd(handle []byte) (int, bool) {
	s.fhMu.Lock()
	defer s.fhMu.Unlock()
	fd, ok := s.fileHandles[string(handle)]
	return fd, ok
}

func (s *session) closeFileHandle(handle []byte) {
	s.fhMu.Lock()
	defer s.fhMu.Unlock()
	handleStr := string(handle)
	if fd, ok := s.fileHandles[handleStr]; ok {
		unix.Close(fd)
		delete(s.fileHandles, handleStr)
	}
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
	resp, err := node.Handle(sess, req)
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
