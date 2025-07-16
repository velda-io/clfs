package vfs

import (
	"context"
	"errors"
	"io"
	"log"
	"sync"
	"syscall"

	"google.golang.org/grpc"
	"velda.io/mtfs/pkg/proto"
)

type Client struct {
	client    proto.MtfsServiceClient
	stream    proto.MtfsService_ServeClient
	mu        sync.Mutex // Protects the stream
	reqId     int64
	callbacks map[int64]OpCallback      // Map of cookie to callback
	notifies  map[string]ServerCallback // Map of cookie to server callback
}

func NewClient(conn *grpc.ClientConn) *Client {
	return &Client{
		client:    proto.NewMtfsServiceClient(conn),
		callbacks: make(map[int64]OpCallback),
		notifies:  make(map[string]ServerCallback),
	}
}

func (c *Client) Start(ctx context.Context) error {
	stream, err := c.client.Serve(ctx)
	if err != nil {
		return err
	}
	c.stream = stream
	return nil
}

func (c *Client) Run(ctx context.Context) error {
	stream := c.stream
	for {
		response, err := stream.Recv()
		debugf("Received response: %v", response)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}
		if response.ServerRequest != nil {
			// This is a server notification
			c.mu.Lock()
			callback, ok := c.notifies[string(response.Cookie)]
			c.mu.Unlock()
			if ok {
				callback(response)
			}
			continue
		} else {
			c.mu.Lock()
			callback, ok := c.callbacks[response.SeqId]
			c.mu.Unlock()
			err = nil
			if response.Error != nil {
				err = syscall.Errno(response.Error.Code)
				response = nil
			}
			if ok {
				callback(response, err)
			}
		}
	}
}

func (c *Client) EnqueueOperation(request *proto.OperationRequest, callback OpCallback) int64 {
	c.mu.Lock()
	c.reqId++
	id := c.reqId
	c.callbacks[id] = callback
	c.mu.Unlock()
	request.SeqId = id
	debugf("Enqueuing operation %d: %v", id, request)
	err := c.stream.Send(request)
	if err != nil {
		log.Printf("Failed to send %d: %v", c.reqId, err)
	}
	return id
}

func (c *Client) RegisterServerCallback(cookie []byte, callback ServerCallback) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.notifies[string(cookie)] = callback
}

func (c *Client) UnregisterServerCallback(cookie []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.notifies, string(cookie))
}
