package vfs

import (
	"context"
	"errors"
	"io"
	"log"
	"sync"
	"syscall"

	"google.golang.org/grpc"
	"velda.io/clfs/pkg/proto"
)

type Client struct {
	client            proto.ClfsServiceClient
	Stream            proto.ClfsService_ServeClient
	mu                sync.Mutex // Protects the stream
	shutdownCond      *sync.Cond // Condition variable to signal shutdown
	shutdown          bool       // Indicates if the client is shutdown
	shutdownCompleted bool       // Indicates if shutdown is completed
	reqId             int64
	callbacks         map[int64]OpCallback // Map of cookie to callback
	inCallback        int
	notifies          map[string]ServerCallback // Map of cookie to server callback
}

func NewClient(conn *grpc.ClientConn) *Client {
	c := &Client{
		client:    proto.NewClfsServiceClient(conn),
		callbacks: make(map[int64]OpCallback),
		notifies:  make(map[string]ServerCallback),
	}
	c.shutdownCond = sync.NewCond(&c.mu)
	return c
}

func (c *Client) Start(ctx context.Context) error {
	stream, err := c.client.Serve(ctx)
	if err != nil {
		return err
	}
	c.Stream = stream
	return nil
}

func (c *Client) Run(ctx context.Context) error {
	stream := c.Stream
	for {
		c.mu.Lock()
		c.inCallback = 0
		if c.shutdown && len(c.callbacks) == 0 {
			c.shutdownCompleted = true
			c.shutdownCond.Broadcast() // Notify shutdown is complete
		}
		c.mu.Unlock()
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
			if ok {
				c.inCallback = 1
			}
			c.mu.Unlock()
			if ok {
				callback(response)
			}
		} else {
			c.mu.Lock()
			callback, ok := c.callbacks[response.SeqId]
			if ok {
				delete(c.callbacks, response.SeqId)
				c.inCallback = 1
			}
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

func (c *Client) Shutdown() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.shutdown = true
	for len(c.callbacks) != 0 || c.inCallback > 0 {
		c.shutdownCond.Wait() // Wait for shutdown to complete
	}
	c.Stream.CloseSend()
}

func (c *Client) EnqueueOperation(request *proto.OperationRequest, callback OpCallback) int64 {
	c.mu.Lock()
	if c.shutdownCompleted {
		panic("Client is already shutdown")
	}
	c.reqId++
	id := c.reqId
	if callback != nil {
		c.callbacks[id] = callback
	}
	c.mu.Unlock()
	request.SeqId = id
	debugf("Enqueuing operation %d: %v", id, request)
	err := c.Stream.Send(request)
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

func (c *Client) ReportAsyncError(fmt string, args ...interface{}) {
	log.Printf("Async error: "+fmt, args...)
}
