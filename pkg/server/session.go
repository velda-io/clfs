package server

import (
	"crypto/rand"
	"sync"

	"golang.org/x/sys/unix"
	"velda.io/clfs/pkg/proto"
)

type ServerRequestCallback func(*proto.OperationRequest, error)

type session struct {
	volume      *Volume // The volume this session is associated with
	streamMu    sync.RWMutex
	stream      proto.ClfsService_ServeServer
	fileHandles map[string]int // map file handle to fd
	fhMu        sync.Mutex

	claimMu      sync.Mutex // Protects the claim tracker
	writerClaims map[*claimTracker]bool
	readerClaims map[*claimTracker]bool

	callbackMu  sync.Mutex                      // Protects callbacks
	serverReqId int64                           // Current request ID for server requests
	callbacks   map[int64]ServerRequestCallback // Map of request IDs to callbacks
}

func NewSession(stream proto.ClfsService_ServeServer, volume *Volume) *session {
	return &session{
		volume:       volume,
		stream:       stream,
		fileHandles:  make(map[string]int),
		writerClaims: make(map[*claimTracker]bool),
		readerClaims: make(map[*claimTracker]bool),
		callbacks:    make(map[int64]ServerRequestCallback),
	}
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
	if req.SeqId < 0 {
		sess.callbackMu.Lock()
		callback, exists := sess.callbacks[req.SeqId]
		delete(sess.callbacks, req.SeqId)
		sess.callbackMu.Unlock()
		if exists {
			callback(req, nil) // Call the callback with no error
		}
		return
	}
	node, err := sess.DecodeCookie(req.Cookie)
	if err != nil {
		sess.streamMu.RLock()
		defer sess.streamMu.RUnlock()
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
	run := func(resp *proto.OperationResponse, err error) {
		debugf("Tx: %d, err %v", req.SeqId, err)
		sess.streamMu.RLock()
		defer sess.streamMu.RUnlock()
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
	debugf("Rx: %d", req.SeqId)
	node.Handle(sess, req, run)
}

func (sess *session) SendNotify(node *ServerNode, op *proto.OperationResponse, callback func(*proto.OperationRequest, error)) {
	sess.streamMu.RLock()
	defer sess.streamMu.RUnlock()
	if sess.stream == nil {
		debugf("Session stream is closed, cannot send notification")
		return
	}
	op.Cookie = sess.GetCookie(node)
	if callback != nil {
		sess.callbackMu.Lock()
		// Use negative IDs for callbacks to avoid conflicts with regular requests
		sess.serverReqId--
		reqId := sess.serverReqId
		sess.callbacks[reqId] = callback
		sess.callbackMu.Unlock()
		op.SeqId = reqId
	}
	if err := sess.stream.Send(op); err != nil {
		debugf("Failed to send notification: %v", err)
	}
}

func (sess *session) DecodeCookie(cookie []byte) (*ServerNode, error) {
	return sess.volume.DecodeCookie(cookie)
}

func (s *session) GetCookie(node *ServerNode) []byte {
	return s.volume.GetCookie(node)
}

func (s *session) Close() {
	func() {
		s.streamMu.Lock()
		defer s.streamMu.Unlock()
		if s.stream != nil {
			s.stream = nil // Mark the stream as closed
		}
	}()

	func() {
		s.fhMu.Lock()
		defer s.fhMu.Unlock()
		for _, fd := range s.fileHandles {
			unix.Close(fd)
		}
		s.fileHandles = nil
	}()
}

func (s *session) AddWriterClaim(tracker *claimTracker) {
	s.claimMu.Lock()
	defer s.claimMu.Unlock()
	s.writerClaims[tracker] = true
}
func (s *session) AddReaderClaim(tracker *claimTracker) {
	s.claimMu.Lock()
	defer s.claimMu.Unlock()
	s.readerClaims[tracker] = true
}
