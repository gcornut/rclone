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
	"io"
	"path"
	"strings"
	"time"
)

// TODO: fix remaining failing integration tests
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
		Description: "IPFS files api",
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
	rootHash        string       // IPFS hash of the root
	initialRootHash string
	_emptyDirHash   string // IPFS hash of an empty dir
}

// Object is a remote object that has been stat'd (so it exists, but is not necessarily open for reading)
type Object struct {
	fs      *Fs
	remote  string
	size    int64
	modTime time.Time
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

	stat, _ := f.api.FilesStat("/")
	if stat != nil {
		fmt.Println("initial root hash", stat.Hash)
		f.rootHash = stat.Hash
		f.initialRootHash = f.rootHash
	} else {
		emptyDirHash, err := f.emptyDirHash()
		if err != nil {
			return nil, err
		}
		f.rootHash = emptyDirHash
	}

	atexit.Register(func() {
		// TODO: extract this code in a separate function
		fmt.Println("final root hash", f.rootHash)

		// Make sure the IPFS MFS was not modified concurrently in the background
		stat, err := f.api.FilesStat("/")
		if err != nil {
			panic(err)
		}
		if stat.Hash != f.initialRootHash {
			panic("Error: concurrent modification of the IPFS MFS. Consistency not guaranteed.")
		}

		// List differences before and after rclone operations
		diff, err := f.api.ObjectDiff(f.initialRootHash, f.rootHash)
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
	})

	return f, nil
}

// Get or fetch he IPFS empty dir hash
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
	return path.Join(f.rootHash, f.relativePath(remote))
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
	return time.Second
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
	objects, err := f.api.Ls(absolutePath)
	if err != nil {
		if _, ok := err.(*api.Error); ok {
			return nil, fs.ErrorDirNotFound
		}
		return nil, err
	}

	modTime := time.Unix(0, 0)
	for _, object := range objects.Objects {
		for _, link := range object.Links {
			remote := path.Join(dir, link.Name)
			if link.Type == api.FileEntryTypeFolder {
				d := fs.NewDir(remote, modTime)
				entries = append(entries, d)
			} else {
				o := &Object{
					fs:      f,
					remote:  remote,
					modTime: modTime,
					size:    link.Size,
				}
				entries = append(entries, o)
			}
		}
	}
	return entries, nil
}

// NewObject finds the Object at remote.  If it can't be found
// it returns the error fs.ErrorObjectNotFound.
func (f *Fs) NewObject(remote string) (fs.Object, error) {
	absolutePath := f.absolutePath(remote)
	stat, err := f.api.ObjectStat(absolutePath)
	if err != nil {
		if _, ok := err.(*api.Error); ok {
			return nil, fs.ErrorObjectNotFound
		}
		return nil, err
	}
	o := &Object{
		fs:     f,
		remote: remote,
		size:   stat.DataSize - 6,
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
	fileAdded, err := f.api.Add(in, file)
	if err != nil {
		return nil, err
	}
	objectPath := f.relativePath(src.Remote())
	result, err := f.api.ObjectPatchAddLink(f.rootHash, objectPath, fileAdded.Hash)
	if err != nil {
		return nil, err
	}
	f.rootHash = result.Hash
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
	result, err := f.api.ObjectPatchAddLink(f.rootHash, dirPath, emptyDirHash)
	if err != nil {
		return err
	}
	f.rootHash = result.Hash
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
	result, err := f.api.ObjectPatchRmLink(f.rootHash, dirPath)
	if err != nil {
		return err
	}
	f.rootHash = result.Hash
	return nil
}

// ------------------------------------------------------------

// Fs returns the parent Fs
func (o *Object) Fs() fs.Info {
	return o.fs
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
	return o.modTime
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
	// TODO: Test if this works with range request (it probably won't) (use /api/v0/cat instead?)
	objectPath := path.Join(o.fs.rootHash, o.Remote())
	in, err = o.fs.api.ObjectData(objectPath, options...)
	return in, err
}

// Update the object with the contents of the io.Reader, modTime and size
//
// If existing is set then it updates the object rather than creating a new one
//
// The new object may have been created if an error is returned
func (o *Object) Update(in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	_, err := o.fs.Put(in, src, options...)
	return err
}

// Remove an object
func (o *Object) Remove() error {
	result, err := o.fs.api.ObjectPatchRmLink(o.fs.rootHash, o.fs.relativePath(o.Remote()))
	if err != nil {
		if _, ok := err.(*api.Error); ok {
			return fs.ErrorObjectNotFound
		}
		return err
	}
	o.fs.rootHash = result.Hash
	return nil
}

// Check the interfaces are satisfied
var (
	_ fs.Fs     = &Fs{}
	_ fs.Object = &Object{}
)
