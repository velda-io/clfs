package server

import (
	"sync"
	"syscall"

	"golang.org/x/sys/unix"
)

type Volume struct {
	dev       uint64
	root      *ServerNode
	mu        sync.Mutex             // Protects the nodes map
	curNodeId int64                  // Current node ID for new nodes
	nodes     map[uint64]*ServerNode // Map of node keys to server nodes
	nodesById map[int64]*ServerNode  // Map of node IDs to server nodes
}

func NewVolume(path string) (*Volume, error) {
	v := &Volume{
		dev:       0,
		nodes:     make(map[uint64]*ServerNode),
		nodesById: make(map[int64]*ServerNode),
	}
	var err error
	v.root, err = v.GetNode(unix.AT_FDCWD, path)
	if err != nil {
		return nil, err
	}
	return v, nil
}

func (v *Volume) GetNode(parentFd int, name string) (*ServerNode, error) {
	openMode := unix.O_PATH | unix.O_NOFOLLOW
	fd, err := unix.Openat(parentFd, name, openMode, 0)
	if err != nil {
		return nil, err
	}
	var stat unix.Stat_t
	err = unix.Fstat(fd, &stat)
	if err != nil {
		unix.Close(fd)
		return nil, err
	}
	key := stat.Ino
	newNode := &ServerNode{
		volume: v,
		fd:     fd,
		nodeId: v.curNodeId,
		mode:   stat.Mode,
		stat:   StatProtoFromSysStat(&stat),
	}
	// Do not allow cross mountpoint
	if stat.Dev != v.dev {
		if v.root == nil {
			v.dev = stat.Dev
			// Setting up root.
		} else {
			unix.Close(fd)
			return nil, syscall.EACCES
		}
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if existingNode, ok := v.nodes[key]; ok {
		unix.Close(fd)
		return existingNode, nil
	}
	newNode.nodeId = v.curNodeId
	v.curNodeId++
	v.nodes[key] = newNode
	v.nodesById[newNode.nodeId] = newNode
	return newNode, nil
}

func (v *Volume) GetNodeById(nodeId int64) *ServerNode {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.nodesById[nodeId]
}
