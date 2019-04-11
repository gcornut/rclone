package ipfs

import (
	"bytes"
	"github.com/ncw/rclone/fs"
	"github.com/ncw/rclone/fs/hash"
	"github.com/ncw/rclone/fs/object"
	"github.com/ncw/rclone/fstest"
	"github.com/stretchr/testify/require"
	"io"
	"testing"
)

func putRandomFile(t *testing.T, f fs.Fs, nBytes int64) fs.Object {
	fileSuffix := fstest.RandomString(100)
	file := fstest.Item{
		ModTime: DefaultModTime,
		Path:    "file-" + fileSuffix + ".txt",
	}

	contents := fstest.RandomString(int(nBytes))
	buf := bytes.NewBufferString(contents)
	hasher := hash.NewMultiHasher()
	in := io.TeeReader(buf, hasher)

	file.Size = int64(buf.Len())
	objInfo := object.NewStaticObjectInfo(file.Path, file.ModTime, file.Size, true, nil, nil)
	obj, err := f.Put(in, objInfo)
	require.NoError(t, err)
	return obj
}

// Check the file size is correctly converted back from IPFS object cumulative size
func testFileSizeConserved(t *testing.T, f fs.Fs, fileSize int64) {
	file := putRandomFile(t, f, fileSize)
	require.Equal(t, fileSize, file.Size())
}

func TestIPFSInternal(t *testing.T) {
	remoteName, _, err := fstest.RandomRemoteName("TestIPFS:")
	require.NoError(t, err)
	f, err := fs.NewFs(remoteName)

	// IPFS object size (or cumulative size) is not the same as the source file size
	// The cumulative size is dependant of the IPFS chunker algorithm
	t.Run("TestFileSizeConversion", func(t *testing.T) {
		// Test small file
		// (sizes know to have different delta between file size and IPFS object cumulative size)
		testFileSizeConserved(t, f, 0)
		testFileSizeConserved(t, f, 1)
		testFileSizeConserved(t, f, 122)
		testFileSizeConserved(t, f, 128)
		testFileSizeConserved(t, f, 16376)
		testFileSizeConserved(t, f, 16384)

		// Test larger file (that are stored in chunks in IPFS)

		// 1 max size chunk and a 1 byte chunk
		testFileSizeConserved(t, f, MaxChunkSize+1)

		// 2 max size chunk
		testFileSizeConserved(t, f, MaxChunkSize*2)

		// 2 max size chunk and 122 bytes chunk
		testFileSizeConserved(t, f, (MaxChunkSize*2)+122)

		// 3 max size chunk and 16376 bytes chunk
		testFileSizeConserved(t, f, (MaxChunkSize*3)+16376)

		// Random size
		//testFileSizeConserved(t, f, rand.Int63n(10000000))
	})
}
