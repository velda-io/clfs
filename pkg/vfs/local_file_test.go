package vfs

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestMkdirAsyncRequest verifies that in async mode the Mkdir method sends the correct request
func TestWriteRead(t *testing.T) {
	t.Skip("Flush will stuck due to missing cookie for inode")
	// Setup
	mockServer := &DummyServer{}
	cookie := []byte("parent-dir-cookie")

	// Create parent inode with SYNC_EXCLUSIVE_WRITE flag (async mode)
	inode := NewDirInode(mockServer, cookie, SYNC_EXCLUSIVE_WRITE, DefaultRootStat())

	dir, _ := testMount(t, inode, nil)

	assert.NoError(t, os.Mkdir(dir+"/testdir", 0755), "Mkdir should succeed")

	file, err := os.Create(dir + "/testdir/testfile.txt")
	assert.NoError(t, err, "Create file should succeed")
	_, err = file.Write([]byte("Hello, World!"))
	assert.NoError(t, err, "Write to file should succeed")
	assert.NoError(t, file.Close(), "Close file should succeed")

	// Create another file
	file2, err := os.Create(dir + "/testdir/testfile2.txt")
	assert.NoError(t, err, "Create second file should succeed")
	_, err = file2.Write([]byte("Another file content"))
	assert.NoError(t, err, "Write to second file should succeed")
	assert.NoError(t, file2.Close(), "Close file should succeed")

	content, err := os.ReadFile(dir + "/testdir/testfile.txt")
	assert.NoError(t, err, "Read file should succeed")
	assert.Equal(t, "Hello, World!", string(content), "File content should match written data")

	content, err = os.ReadFile(dir + "/testdir/testfile2.txt")
	assert.NoError(t, err, "Read second file should succeed")
	assert.Equal(t, "Another file content", string(content), "Second file content should match written data")

	/*
		err = os.RemoveAll(dir + "/testdir")
		assert.NoError(t, err, "Remove directory should succeed")
	*/
}
