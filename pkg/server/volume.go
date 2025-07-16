package server

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"syscall"

	lru "github.com/hashicorp/golang-lru/v2"
	"golang.org/x/sys/unix"
)

type Volume struct {
	dev           uint64
	rootFd        int // File descriptor for the root directory
	root          *ServerNode
	mu            sync.Mutex                      // Protects the nodes map
	curNodeId     int64                           // Current node ID for new nodes
	nodes         map[uint64]*ServerNode          // Map of node Ino to server nodes
	nodesByCookie *lru.Cache[string, *ServerNode] // Cache for nodes by cookie

}

func NewVolume(path string) (*Volume, error) {
	v := &Volume{
		dev:   0,
		nodes: make(map[uint64]*ServerNode),
	}
	var err error
	v.nodesByCookie, err = lru.NewWithEvict(1000, func(key string, value *ServerNode) {
		v.mu.Lock()
		defer v.mu.Unlock()
		if value == v.root {
			go func() {
				v.nodesByCookie.Add(key, v.root) // Re-add root node to cache
			}()
			return
		}
		delete(v.nodes, value.nodeId)
		value.Close()
	})
	if err != nil {
		return nil, err
	}
	v.rootFd, err = unix.Open(path, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	var stat unix.Stat_t
	err = unix.Fstat(v.rootFd, &stat)
	if err != nil {
		unix.Close(v.rootFd)
		return nil, err
	}
	v.dev = stat.Dev

	v.root, err = v.GetNode(unix.AT_FDCWD, path)
	if err != nil {
		return nil, err
	}
	return v, nil
}

func (v *Volume) GetNode(parentFd int, name string) (*ServerNode, error) {
	fileHandle, _, err := unix.NameToHandleAt(parentFd, name, 0)
	if err != nil {
		return nil, err
	}
	return v.GetNodeByHandle(fileHandle)
}

func (v *Volume) GetNodeByHandle(fileHandle unix.FileHandle) (*ServerNode, error) {
	fd, err := unix.OpenByHandleAt(v.rootFd, fileHandle, unix.O_PATH|unix.O_NOFOLLOW)
	if err != nil {
		return nil, fmt.Errorf("failed to open file handle: %w", err)
	}
	var stat unix.Stat_t
	err = unix.Fstat(fd, &stat)
	if err != nil {
		unix.Close(fd)
		return nil, err
	}
	key := stat.Ino
	// Do not allow cross mountpoint
	if stat.Dev != v.dev {
		unix.Close(fd)
		return nil, syscall.EACCES
	}
	newNode := &ServerNode{
		volume: v,
		fh:     fileHandle,
		fd:     fd,
		nodeId: key,
	}
	newNode.tracker = NewClaimTracker(newNode)
	v.mu.Lock()
	defer v.mu.Unlock()
	if existingNode, ok := v.nodes[key]; ok {
		unix.Close(fd)
		return existingNode, nil
	}
	v.nodes[key] = newNode
	return newNode, nil
}

func (v *Volume) GetCookie(node *ServerNode) []byte {
	buf := bytes.NewBuffer(nil)
	binary.Write(buf, binary.LittleEndian, int32(node.fh.Type()))
	binary.Write(buf, binary.LittleEndian, int32(node.fh.Size()))
	buf.Write(node.fh.Bytes())
	data := buf.Bytes()
	v.nodesByCookie.Add(string(data), node)
	// TODO: Encrypt the data to avoid spoofing.
	return data
}

var ErrInvalidCookie = errors.New("invalid cookie")

func (v *Volume) DecodeCookie(cookie []byte) (*ServerNode, error) {
	if n, ok := v.nodesByCookie.Get(string(cookie)); ok {
		return n, nil
	}
	var t, size int32

	buf := bytes.NewBuffer(cookie)
	if err := binary.Read(buf, binary.LittleEndian, &t); err != nil {
		return nil, fmt.Errorf("invalid cookie: %v %w", ErrInvalidCookie, err)
	}
	if err := binary.Read(buf, binary.LittleEndian, &size); err != nil {
		return nil, fmt.Errorf("invalid cookie: %v %w", ErrInvalidCookie, err)
	}
	data := make([]byte, size)
	if err := binary.Read(buf, binary.LittleEndian, data); err != nil {
		return nil, fmt.Errorf("invalid cookie: %v %w", ErrInvalidCookie, err)
	}
	if buf.Len() != 0 {
		return nil, fmt.Errorf("cookie has extra data: %d bytes", buf.Len())
	}
	fh := unix.NewFileHandle(t, data)
	node, err := v.GetNodeByHandle(fh)
	if err != nil {
		return nil, err
	}
	v.nodesByCookie.Add(string(cookie), node)
	return node, nil
}
