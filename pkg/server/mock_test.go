package server

import (
	"testing"

	"velda.io/clfs/pkg/proto"
)

// mockStream is a mock implementation of proto.ClfsService_ServeServer
type mockStream struct {
	proto.ClfsService_ServeServer
}

func (m *mockStream) Send(*proto.OperationResponse) error {
	return nil
}

// createMockSession creates a session that can be used for testing
func createMockSession(t *testing.T) *session {
	t.Helper()
	return NewSession(&mockStream{}, nil)
}
