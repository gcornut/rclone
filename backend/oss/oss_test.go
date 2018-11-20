// Test OSS filesystem interface
package oss

import (
	"testing"

	"github.com/ncw/rclone/fstest/fstests"
)

// TestIntegration runs integration tests against the remote
func TestIntegration(t *testing.T) {
	fstests.Run(t, &fstests.Opt{
		RemoteName: "TestOss:",
		NilObject:  (*Object)(nil),
		// ChunkedUpload: fstests.ChunkedUploadConfig{
		// 	MinChunkSize: minChunkSize,
		// },
	})
}

// func (f *Fs) SetUploadChunkSize(cs fs.SizeSuffix) (fs.SizeSuffix, error) {
// 	return f.setUploadChunkSize(cs)
// }

// var _ fstests.SetUploadChunkSizer = (*Fs)(nil)
