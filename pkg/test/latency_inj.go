package test

import (
	"log"
	"time"

	"velda.io/clfs/pkg/proto"
)

type queuedItem struct {
	time time.Time
	req  *proto.OperationRequest
}

type LatencyInjectedStream struct {
	proto.ClfsService_ServeClient
	latency time.Duration
	ch      chan *queuedItem
}

func NewLatencyInjectedStream(stream proto.ClfsService_ServeClient, latency time.Duration) *LatencyInjectedStream {
	s := &LatencyInjectedStream{
		ClfsService_ServeClient: stream,
		latency:                 latency,
		ch:                      make(chan *queuedItem, 5000), // Buffered channel to hold requests
	}
	go s.Run()
	return s
}

func (s *LatencyInjectedStream) Send(req *proto.OperationRequest) error {
	s.ch <- &queuedItem{
		time: time.Now().Add(s.latency),
		req:  req,
	} // Send request to the channel

	return nil
}

func (s *LatencyInjectedStream) CloseSend() error {
	close(s.ch)                                  // Close the channel to signal no more requests
	return s.ClfsService_ServeClient.CloseSend() // Call the original CloseSend
}

func (s *LatencyInjectedStream) Run() {
	for {
		item, ok := <-s.ch // Wait for a request
		if !ok {
			log.Println("LatencyInjectedStream closed")
			return
		}
		time.Sleep(time.Until(item.time))
		if err := s.ClfsService_ServeClient.Send(item.req); err != nil {
			log.Fatalf("Failed to send request: %v", err)
			return
		}
	}
}
