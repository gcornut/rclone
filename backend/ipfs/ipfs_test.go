// Test IPFS filesystem interface
package ipfs_test

import (
	"testing"

	"github.com/ncw/rclone/backend/ipfs"
	"github.com/ncw/rclone/fstest/fstests"
)

// TestIntegration runs integration tests against the remote
func TestIntegration(t *testing.T) {
	fstests.Run(t, &fstests.Opt{
		RemoteName: "TestIPFS:",
		NilObject:  (*ipfs.Object)(nil),
	})
}
