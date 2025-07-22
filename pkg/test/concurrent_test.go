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
		stat, err := os.Stat(dir1 + "/testdir/testfile.txt")
		assert.NoError(t, err, "Stat file should succeed")
		assert.Equal(t, stat.Size(), int64(13), "File size should match written data size")

		// Rewrite on another client.
		file2, err := os.Create(dir2 + "/testdir/testfile.txt")
		assert.NoError(t, err, "Create second file should succeed")
		_, err = file2.Write([]byte("Another file content"))
		assert.NoError(t, err, "Write to second file should succeed")
		assert.NoError(t, file2.Close(), "Close file should succeed")
		stat3, err := os.Stat(dir2 + "/testdir/testfile.txt")
		assert.NoError(t, err, "Stat second file should succeed")

		content, err := os.ReadFile(dir1 + "/testdir/testfile.txt")
		assert.NoError(t, err, "Read second file should succeed")
		assert.Equal(t, "Another file content", string(content), "Second file content should match written data")
		stat2, err := os.Stat(dir2 + "/testdir/testfile.txt")
		assert.NoError(t, err, "Stat second file should succeed")
		assert.NotEqual(t, stat.ModTime(), stat2.ModTime(), "Modification times should differ after concurrent write")
		assert.Equal(t, stat2.Size(), int64(20), "Second file size should match written data size")
		assert.Equal(t, stat3.ModTime(), stat2.ModTime(), "Modification time of second file should match")

		err = os.RemoveAll(dir1 + "/testdir")
		assert.NoError(t, err, "Remove directory should succeed")

		_, err = os.Stat(dir2 + "/testdir")
		assert.ErrorIs(t, err, unix.ENOENT, "Directory should not exist after removal %v:", err)
	})

}
