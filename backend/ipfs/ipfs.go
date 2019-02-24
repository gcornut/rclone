package ipfs

import (
	"fmt"
	"github.com/ncw/rclone/backend/ipfs/api"
	"github.com/ncw/rclone/fs"
	"github.com/ncw/rclone/fs/config/configmap"
	"github.com/ncw/rclone/fs/config/configstruct"
	"github.com/ncw/rclone/fs/fshttp"
	"github.com/ncw/rclone/fs/hash"
	"github.com/ncw/rclone/lib/atexit"
	"github.com/pkg/errors"
	"io"
	"path"
	"strings"
	"time"
)

// TODO: handle hash pinning (pin on file add, pin update on MFS update, unpin individual files)
// TODO: implement optional Fs interfaces
// TODO: add read only access to `/ipfs/<HASH>` paths (via `--ipfs-root=<>` option)
// TODO: add read/write  access to `/ipns/<HASH>` paths (via `--ipfs-root=<>` option)
// TODO: add periodic flush to mutable FS (MFS or IPNS) rather than only at the end
// TODO: export api calls to `go-ipfs-api`?
// TODO: add new hash type (compute local IPFS hashes)
// TODO: write documentation for IPFS backend

// Register with Fs
func init() {
	fsi := &fs.RegInfo{
		Name:        "ipfs",
		Description: "IPFS",
		NewFs:       NewFs,
		Options: []fs.Option{{
			Name:     "url",
			Help:     "URL of http IPFS API server",
			Required: true,
			Examples: []fs.OptionExample{{
				Value: "http://localhost:5001",
				Help:  "Connect to your local IPFS API server",
			}},
		}},
	}
	fs.Register(fsi)
}

// Max size of a IPFS object data after which the IPFS chunker will
// chunk the original file
const IpfsMaxChunkSize = int64(262144)

var (
	DefaultTime   = time.Unix(0, 0)
	rootHashCache = make(map[string]string)
)

// ------------------------------------------------------------

// Options defines the configuration for this backend
type Options struct {
	Endpoint string `config:"url"`
}

// Fs stores the interface to the remote HTTP files
type Fs struct {
	name            string
	root            string
	features        *fs.Features // optional features
	opt             Options      // options for this backend
	api             *api.Api     // the connection to the server
	_rootHash       string       // IPFS hash of the root
	initialRootHash string
	_emptyDirHash   string // IPFS hash of an empty dir
}

// Object is a remote object that has been stat'd (so it exists, but is not necessarily open for reading)
type Object struct {
	fs     *Fs
	remote string
	size   int64
}

// ------------------------------------------------------------

// NewFs creates a new Fs object from the name and root. It connects to
// the host specified in the config file.
func NewFs(name string, root string, m configmap.Mapper) (fs.Fs, error) {
	opt := new(Options)
	err := configstruct.Set(m, opt)
	if err != nil {
		return nil, err
	}

	client := fshttp.NewClient(fs.Config)

	f := &Fs{
		name: name,
		root: root,
		opt:  *opt,
		api:  api.NewApi(client, opt.Endpoint),
	}
	f.features = (&fs.Features{
		CaseInsensitive:         true,
		CanHaveEmptyDirectories: true,
	}).Fill(f)

	// Initialize IPFS root HASH
	err = f.initializeRootHash()
	if err != nil {
		return nil, err
	}

	// Persist Fs changes to IPFS MFS on program exit
	atexit.Register(f.persistToMFS)

	var fsError error = nil
	if root != "" {
		// Check to see if the root actually an existing file
		remote := path.Base(root)
		f.root = path.Dir(root)
		if f.root == "." {
			f.root = ""
		}

		_, err := f.NewObject(remote)
		if err != nil {
			if err == fs.ErrorObjectNotFound || errors.Cause(err) == fs.ErrorNotAFile || err == fs.ErrorNotAFile {
				// Remote is file or doesn't exist => reset root
				f.root = root
			} else {
				return nil, err
			}
		} else {
			// return an error with an fs which points to the parent
			fsError = fs.ErrorIsFile
		}
	}

	return f, fsError
}

func (f *Fs) initializeRootHash() error {
	cachedRootHash, ok := rootHashCache["/"]
	if ok {
		f._rootHash = cachedRootHash
	}
	if f._rootHash == "" {
		stat, err := f.api.FilesStat("/")
		if err != nil {
			return err
		}
		f._rootHash = stat.Hash
		rootHashCache["/"] = stat.Hash
	}
	f.initialRootHash = f._rootHash
	return nil
}

// Get or fetch the IPFS empty directory hash
func (f *Fs) emptyDirHash() (string, error) {
	if f._emptyDirHash == "" {
		result, err := f.api.ObjectNewDir()
		if err != nil {
			return "", err
		}
		f._emptyDirHash = result.Hash
	}
	return f._emptyDirHash, nil
}

func (f *Fs) relativePath(remote string) (relativePath string) {
	relativePath = path.Join(f.root, remote)
	if strings.HasPrefix(relativePath, "/") {
		// Should not start with "/"
		relativePath = relativePath[1:]
	}
	return relativePath
}

func (f *Fs) absolutePath(remote string) (relativePath string) {
	return path.Join(f._rootHash, f.relativePath(remote))
}

// Check if IPFS remote is a file (file type can only be obtained by
// listing files of the parent directory)
func (f *Fs) isFile(remote string) (error, bool) {
	absolutePath := f.absolutePath(remote)
	dir, file := path.Split(absolutePath)
	if dir == f._rootHash {
		// root dir => not a file
		return nil, false
	} else {
		links, err := f.api.Ls(dir)
		if err != nil {
			return err, false
		}
		for _, link := range links {
			if link.Name == file {
				return nil, link.Type == api.FileEntryTypeFile
			}
		}
		return nil, false
	}
}

// Convert IPFS object cumulative size to actual file size
// Only for small file of size < 262267
func convertSmallFileSize(ipfsCumulativeSize int64) int64 {
	switch {
	case ipfsCumulativeSize == 0:
		return 0
	case ipfsCumulativeSize < 9:
		return ipfsCumulativeSize - 6
	case ipfsCumulativeSize < 131:
		return ipfsCumulativeSize - 8
	case ipfsCumulativeSize < 139:
		return ipfsCumulativeSize - 9
	case ipfsCumulativeSize < 16388:
		return ipfsCumulativeSize - 11
	case ipfsCumulativeSize < 16398:
		return ipfsCumulativeSize - 12
	default:
		return ipfsCumulativeSize - 14
	}
}

// Convert IPFS object size to actual file size
func (f *Fs) convertToFileSize(objectStat api.ObjectStat) int64 {
	// Calculate file size
	var fileSize int64
	cumulativeSize := objectStat.CumulativeSize
	if cumulativeSize < (IpfsMaxChunkSize + 123) {
		// Single chunk file
		fileSize = convertSmallFileSize(cumulativeSize)
	} else {
		// Multiple chunk file
		i := cumulativeSize - objectStat.BlockSize
		maxSizeChunks := i / (IpfsMaxChunkSize + 14)
		remainingSizeChunk := i % (IpfsMaxChunkSize + 14)
		fileSize = i - (maxSizeChunks * 14) - (remainingSizeChunk - convertSmallFileSize(remainingSizeChunk))
	}
	return fileSize
}

// Persist modified IPFS DAG to IPFS MFS
func (f *Fs) persistToMFS() {
	fmt.Println("final root hash", f._rootHash)

	// Make sure the IPFS MFS was not modified concurrently in the background
	stat, err := f.api.FilesStat("/")
	if err != nil {
		panic(err)
	}
	if stat.Hash != f.initialRootHash {
		panic("Error: concurrent modification of the IPFS MFS. Consistency not guaranteed.")
	}

	// List differences before and after rclone operations
	diff, err := f.api.ObjectDiff(f.initialRootHash, f._rootHash)
	if err != nil {
		panic(err)
	}

	// Persist changes to IPFS MFS
	for _, change := range diff.Changes {
		if change.Before != nil {
			err := f.api.FilesRm("/" + change.Path)
			if err != nil {
				panic(err)
			}
		}
		if change.After != nil {
			absolutePath := "/ipfs/" + change.After["/"]
			err := f.api.FilesCp(absolutePath, "/"+change.Path)
			if err != nil {
				panic(err)
			}
		}
	}
}

// Name of the remote (as passed into NewFs)
func (f *Fs) Name() string {
	return f.name
}

// Root of the remote (as passed into NewFs)
func (f *Fs) Root() string {
	return f.root
}

// String converts this Fs to a string
func (f *Fs) String() string {
	return fmt.Sprintf("ipfs files root '%s'", f.root)
}

// Features returns the optional features of this Fs
func (f *Fs) Features() *fs.Features {
	return f.features
}

// Hashes returns the supported hash sets.
func (f *Fs) Hashes() hash.Set {
	return hash.Set(hash.None)
}

// Precision return the precision of this Fs
func (f *Fs) Precision() time.Duration {
	return fs.ModTimeNotSupported
}

// List the objects and directories in dir into entries.  The
// entries can be returned in any order but should be for a
// complete directory.
//
// dir should be "" to list the root, and should not have
// trailing slashes.
//
// This should return ErrorDirNotFound if the directory isn't
// found.
func (f *Fs) List(dir string) (entries fs.DirEntries, err error) {
	absolutePath := f.absolutePath(dir)
	links, err := f.api.Ls(absolutePath)
	if err != nil {
		if _, ok := err.(*api.Error); ok {
			return nil, fs.ErrorDirNotFound
		}
		return nil, err
	}

	for _, link := range links {
		remote := path.Join(dir, link.Name)
		if link.Type == api.FileEntryTypeFolder {
			d := fs.NewDir(remote, DefaultTime)
			entries = append(entries, d)
		} else {
			stat, err := f.api.ObjectStat(f.absolutePath(remote))
			if err != nil {
				return nil, err
			}
			o := &Object{
				fs:     f,
				remote: remote,
				size:   f.convertToFileSize(*stat),
			}
			entries = append(entries, o)
		}
	}
	return entries, nil
}

// NewObject finds the Object at remote.  If it can't be found
// it returns the error fs.ErrorObjectNotFound. If is a directory
func (f *Fs) NewObject(remote string) (fs.Object, error) {
	absolutePath := f.absolutePath(remote)
	stat, err := f.api.ObjectStat(absolutePath)
	if err != nil {
		if _, ok := err.(*api.Error); ok {
			return nil, fs.ErrorObjectNotFound
		}
		return nil, err
	}
	_, isFile := f.isFile(remote)
	if !isFile {
		return nil, fs.ErrorNotAFile
	}
	o := &Object{
		fs:     f,
		remote: remote,
		size:   f.convertToFileSize(*stat),
	}
	return o, nil
}

// Put the object
//
// Copy the reader in to the new object which is returned
//
// The new object may have been created if an error is returned
func (f *Fs) Put(in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	_, file := path.Split(src.Remote())
	fileAdded, err := f.api.Add(in, file, options...)
	if err != nil {
		return nil, err
	}
	objectPath := f.relativePath(src.Remote())
	result, err := f.api.ObjectPatchAddLink(f._rootHash, objectPath, fileAdded.Hash)
	if err != nil {
		return nil, err
	}
	f.SetRootHash(result.Hash)
	o, err := f.NewObject(src.Remote())
	if err != nil {
		return nil, err
	}
	return o, nil
}

// Mkdir creates the container if it doesn't exist
func (f *Fs) Mkdir(dir string) error {
	emptyDirHash, err := f.emptyDirHash()
	if err != nil {
		return err
	}

	dirPath := f.relativePath(dir)
	result, err := f.api.ObjectPatchAddLink(f._rootHash, dirPath, emptyDirHash)
	if err != nil {
		return err
	}
	f.SetRootHash(result.Hash)
	return nil
}

// Rmdir deletes the root folder
//
// Returns ErrorDirectoryNotEmpty if it isn't empty
func (f *Fs) Rmdir(dir string) error {
	absolutePath := f.absolutePath(dir)
	stat, err := f.api.ObjectStat(absolutePath)
	if err != nil {
		if _, ok := err.(*api.Error); ok {
			return fs.ErrorDirNotFound
		}
		return err
	}
	// Should not have children
	if stat.NumLinks > 0 {
		return fs.ErrorDirectoryNotEmpty
	}

	dirPath := f.relativePath(dir)
	result, err := f.api.ObjectPatchRmLink(f._rootHash, dirPath)
	if err != nil {
		return err
	}
	f.SetRootHash(result.Hash)
	return nil
}

func (f *Fs) SetRootHash(rootHash string) {
	rootHashCache["/"] = rootHash
	f._rootHash = rootHash
}

// ------------------------------------------------------------

// Fs returns the parent Fs
func (o *Object) Fs() fs.Info {
	return o.fs
}

func (o *Object) relativePath() string {
	return o.fs.relativePath(o.Remote())
}

func (o *Object) absolutePath() string {
	return o.fs.absolutePath(o.Remote())
}

// Return a string version
func (o *Object) String() string {
	if o == nil {
		return "<nil>"
	}
	return o.remote
}

// Remote returns the remote path
func (o *Object) Remote() string {
	return o.remote
}

// Hash returns the SHA-1 of an object returning a lowercase hex string
func (o *Object) Hash(t hash.Type) (string, error) {
	return "", hash.ErrUnsupported
}

// Size returns the size of an object in bytes
func (o *Object) Size() int64 {
	return o.size
}

// ModTime returns the modification time of the object
func (o *Object) ModTime() time.Time {
	return DefaultTime
}

// SetModTime sets the modification time of the local fs object
func (o *Object) SetModTime(modTime time.Time) error {
	// Addition of modification time on IPFS is discussed at:
	// https://github.com/ipfs/unixfs-v2/issues/1
	return fs.ErrorCantSetModTime
}

// Storable returns a boolean showing whether this object storable
func (o *Object) Storable() bool {
	return true
}

// Open an object for read
func (o *Object) Open(options ...fs.OpenOption) (in io.ReadCloser, err error) {
	return o.fs.api.Cat(o.absolutePath(), o.Size(), options...)
}

// Update the object with the contents of the io.Reader, modTime and size
//
// If existing is set then it updates the object rather than creating a new one
//
// The new object may have been created if an error is returned
func (o *Object) Update(in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	o2, err := o.fs.Put(in, src, options...)
	o.size = o2.Size()
	return err
}

// Remove an object
func (o *Object) Remove() error {
	result, err := o.fs.api.ObjectPatchRmLink(o.fs._rootHash, o.relativePath())
	if err != nil {
		if _, ok := err.(*api.Error); ok {
			return fs.ErrorObjectNotFound
		}
		return err
	}
	o.fs.SetRootHash(result.Hash)
	return nil
}

// Check the interfaces are satisfied
var (
	_ fs.Fs     = &Fs{}
	_ fs.Object = &Object{}
)
