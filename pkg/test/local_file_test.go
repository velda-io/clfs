package test

import (
	"os"
	"os/exec"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func remount(dir string, remount bool, t *testing.T, s *TestServer, mode int, latency time.Duration) string {
	if remount {
		return Mount(t, s, mode, latency)
	}
	return dir
}
func TestWriteThenRead(t *testing.T) {
	mode := 0
	tests := []struct {
		name    string
		remount bool
		latency time.Duration
	}{
		{"Default", false, 0},
		{"Remount", true, 0},
		{"Latency", false, 500 * time.Millisecond},
	}
	for _, tt := range tests {
		t.Run("TestRequest"+tt.name, func(t *testing.T) {
			// Setup
			s := StartTestServer(t)

			dir := Mount(t, s, mode, tt.latency)

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

			dir = remount(dir, tt.remount, t, s, mode, tt.latency)

			content, err := os.ReadFile(dir + "/testdir/testfile.txt")
			assert.NoError(t, err, "Read file should succeed")
			assert.Equal(t, "Hello, World!", string(content), "File content should match written data")

			content, err = os.ReadFile(dir + "/testdir/testfile2.txt")
			assert.NoError(t, err, "Read second file should succeed")
			assert.Equal(t, "Another file content", string(content), "Second file content should match written data")

			err = os.RemoveAll(dir + "/testdir")
			assert.NoError(t, err, "Remove directory should succeed")
		})

		t.Run("TestFStat"+tt.name, func(t *testing.T) {
			s := StartTestServer(t)
			dir := Mount(t, s, mode, tt.latency)

			file, err := os.Create(dir + "/testfile.txt")
			assert.NoError(t, err, "Create file should succeed")
			_, err = file.Write([]byte("Hello, World!"))
			assert.NoError(t, err, "Write to file should succeed")
			assert.NoError(t, file.Close(), "Close file should succeed")

			dir = remount(dir, tt.remount, t, s, mode, tt.latency)

			fi, err := os.Stat(dir + "/testfile.txt")
			assert.NoError(t, err, "Stat file should succeed")
			assert.Equal(t, "testfile.txt", fi.Name(), "File name should match")
			assert.Equal(t, int64(13), fi.Size(), "File size should match written data size")
		})

		t.Run("TestListDir"+tt.name, func(t *testing.T) {
			s := StartTestServer(t)
			dir := Mount(t, s, mode, tt.latency)

			err := os.Mkdir(dir+"/testdir", 0755)
			assert.NoError(t, err, "Mkdir should succeed")

			file, err := os.Create(dir + "/testdir/testfile.txt")
			assert.NoError(t, err, "Create file should succeed")
			_, err = file.Write([]byte("Hello, World!"))
			assert.NoError(t, err, "Write to file should succeed")
			assert.NoError(t, file.Close(), "Close file should succeed")

			dir = remount(dir, tt.remount, t, s, mode, tt.latency)

			files, err := os.ReadDir(dir + "/testdir")
			assert.NoError(t, err, "ReadDir should succeed")
			assert.Len(t, files, 1, "Directory should contain one file")
			assert.Equal(t, "testfile.txt", files[0].Name(), "File name in directory should match")
		})

		t.Run("TestSetAttr"+tt.name, func(t *testing.T) {
			s := StartTestServer(t)
			dir := Mount(t, s, mode, tt.latency)

			file, err := os.Create(dir + "/testfile.txt")
			assert.NoError(t, err, "Create file should succeed")
			_, err = file.Write([]byte("Hello, World!"))
			assert.NoError(t, err, "Write to file should succeed")
			assert.NoError(t, file.Close(), "Close file should succeed")

			err = os.Chmod(dir+"/testfile.txt", 0644)
			assert.NoError(t, err, "Chmod should succeed")

			dir = remount(dir, tt.remount, t, s, mode, tt.latency)

			fi, err := os.Stat(dir + "/testfile.txt")
			assert.NoError(t, err, "Stat file should succeed")
			assert.Equal(t, os.FileMode(0644), fi.Mode().Perm(), "File permissions should match")
		})

		t.Run("TestExecFile"+tt.name, func(t *testing.T) {
			s := StartTestServer(t)
			dir := Mount(t, s, mode, tt.latency)

			file, err := os.Create(dir + "/run.sh")
			assert.NoError(t, err, "Create file should succeed")
			_, err = file.Write([]byte("#!/bin/sh\necho 'Hello, World!'"))
			assert.NoError(t, err, "Write to file should succeed")
			assert.NoError(t, file.Close(), "Close file should succeed")

			// Simulate execution (this is a dummy test as we can't execute files in this context)
			err = os.Chmod(dir+"/run.sh", 0755)
			assert.NoError(t, err, "Chmod for execution should succeed")

			dir = remount(dir, tt.remount, t, s, mode, tt.latency)

			cmd := exec.Command("sh", dir+"/run.sh")
			output, err := cmd.CombinedOutput()
			assert.NoError(t, err, "Command execution should succeed")
			assert.Equal(t, "Hello, World!\n", string(output), "Command output should match")
		})

		// A newly created directory should have a write-exclusive claim, so all writes are async.
		t.Run("TestCreateLargeDir"+tt.name, func(t *testing.T) {
			s := StartTestServer(t)
			dir := Mount(t, s, mode, tt.latency)

			err := os.Mkdir(dir+"/large-dir", 0755)
			assert.NoError(t, err, "Mkdir for large directory should succeed")
			// Create a large directory with many files
			for i := 0; i < 1000; i++ {
				file, err := os.Create(dir + "/large-dir/file" + strconv.Itoa(i) + ".txt")
				assert.NoError(t, err, "Create file should succeed")
				_, err = file.Write([]byte("This is file number " + strconv.Itoa(i)))
				assert.NoError(t, err, "Write to file should succeed")
				assert.NoError(t, file.Close(), "Close file should succeed")
			}

			dir = remount(dir, tt.remount, t, s, mode, 0)

			files, err := os.ReadDir(dir + "/large-dir")
			assert.NoError(t, err, "ReadDir should succeed")
			assert.Len(t, files, 1000, "Directory should contain 1000 files")
			// Read a few files to verify content
			for i := 900; i < 910; i++ {
				content, err := os.ReadFile(dir + "/large-dir/file" + strconv.Itoa(i) + ".txt")
				assert.NoError(t, err, "Read file should succeed")
				assert.Equal(t, "This is file number "+strconv.Itoa(i), string(content), "File content should match written data")
			}
		})
	}
}
