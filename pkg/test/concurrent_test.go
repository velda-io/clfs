package test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"golang.org/x/sys/unix"
)

func TestConcurrentAccess(t *testing.T) {
	t.Run("TestWriteOnOtherClient", func(t *testing.T) {
		// Setup
		s := StartTestServer(t)

		dir1 := Mount(t, s, 0, 0)
		dir2 := Mount(t, s, 0, 0)

		assert.NoError(t, os.Mkdir(dir1+"/testdir", 0755), "Mkdir should succeed")

		file, err := os.Create(dir1 + "/testdir/testfile.txt")
		assert.NoError(t, err, "Create file should succeed")
		_, err = file.Write([]byte("Hello, World!"))
		assert.NoError(t, err, "Write to file should succeed")
		assert.NoError(t, file.Close(), "Close file should succeed")

		// Rewrite on another client.
		file2, err := os.Create(dir2 + "/testdir/testfile.txt")
		assert.NoError(t, err, "Create second file should succeed")
		_, err = file2.Write([]byte("Another file content"))
		assert.NoError(t, err, "Write to second file should succeed")
		assert.NoError(t, file2.Close(), "Close file should succeed")

		content, err := os.ReadFile(dir1 + "/testdir/testfile.txt")
		assert.NoError(t, err, "Read second file should succeed")
		assert.Equal(t, "Another file content", string(content), "Second file content should match written data")

		err = os.RemoveAll(dir1 + "/testdir")
		assert.NoError(t, err, "Remove directory should succeed")

		_, err = os.Stat(dir2 + "/testdir")
		assert.ErrorIs(t, err, unix.ENOENT, "Directory should not exist after removal %v:", err)
	})

}
