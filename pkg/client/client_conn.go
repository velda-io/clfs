package client

import (
	"context"
	"sync"

	"google.golang.org/grpc"
	"velda.io/mtfs/pkg/proto"
	"velda.io/mtfs/pkg/vfs"
)

type Client struct {
	client    proto.MtfsServiceClient
	stream    proto.MtfsService_ServeClient
	mu        sync.Mutex // Protects the stream
	reqId     int64
	callbacks map[int64]vfs.OpCallback      // Map of cookie to callback
	notifies  map[string]vfs.ServerCallback // Map of cookie to server callback
}

func NewClient(conn *grpc.ClientConn) *Client {
	return &Client{
		client:    proto.NewMtfsServiceClient(conn),
		callbacks: make(map[int64]vfs.OpCallback),
		notifies:  make(map[string]vfs.ServerCallback),
	}
}

func (c *Client) Run(ctx context.Context) error {
	stream, err := c.client.Serve(ctx)
	if err != nil {
		return err
	}
	c.stream = stream

	for {
		response, err := stream.Recv()
		if err != nil {
			return err
		}
		if response.SeqId == 0 {
			// This is a server callback
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
			if ok {
				callback(response, nil)
			}
		}
	}
}

func (c *Client) EnqueueOperation(request *proto.OperationRequest, callback vfs.OpCallback) int64 {
	c.mu.Lock()
	c.reqId++
	id := c.reqId
	c.callbacks[id] = callback
	c.mu.Unlock()
	request.SeqId = id
	c.stream.Send(request)
	return id
}

func (c *Client) RegisterServerCallback(cookie []byte, callback vfs.ServerCallback) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.notifies[string(cookie)] = callback
}

func (c *Client) UnregisterServerCallback(cookie []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.notifies, string(cookie))
}
