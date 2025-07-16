package server

import (
	"crypto/rand"
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

func (sess *session) HandleOp(req *proto.OperationRequest) {
	node, err := sess.DecodeCookie(req.Cookie)
	if err != nil {
		sess.stream.Send(&proto.OperationResponse{
			Error: &proto.ErrorResponse{
				DetailMsg: err.Error(),
				Code:      int32(unix.EINVAL),
			},
		})
		return
	}
	sess.handleOp(node, req)
}

func (sess *session) handleOp(node *ServerNode, req *proto.OperationRequest) {
	debugf("Rx: %d", req.SeqId)
	resp, err := node.Handle(sess, req)
	debugf("Tx: %d, err %v", req.SeqId, err)
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
		debugf("Stream failure: %v", err)
	}
}

func (sess *session) DecodeCookie(cookie []byte) (*ServerNode, error) {
	return sess.volume.DecodeCookie(cookie)
}

func (s *session) GetCookie(node *ServerNode) []byte {
	return s.volume.GetCookie(node)
}
