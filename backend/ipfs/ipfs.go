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
	"sync"
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
const MaxChunkSize = int64(262144)

const HttpGateway = "https://ipfs.io/ipfs/"

type SharedRoots struct {
	sync.RWMutex

	// Map from ENDPOINT:IPFS_PATH to IPFS Root
	// example of map key "http://localhost:5001:/"
	// => IPFS MFS on local IPFS node
	cache map[string]*Root
}

type Root struct {
	sync.RWMutex
	api         *api.Api
	initialHash string
	hash        string
	log         func(text string, args ...interface{})
}

func NewRoot(f *Fs, ipfsPath string) (*Root, error) {
	stat, err := f.api.FilesStat(ipfsPath)
	if err != nil {
		return nil, err
	}

	r := Root{
		api:         f.api,
		initialHash: stat.Hash,
		hash:        stat.Hash,
		log: func(text string, args ...interface{}) {
			fs.Logf(f, text, args...)
		},
	}

	// Persist Fs changes to IPFS MFS on program exit
	atexit.Register(r.persistToMFS)

	// Also persist periodically
	r.periodicPersist()
	return &r, nil
}

// Persist modified IPFS DAG to IPFS MFS
func (r *Root) persistToMFS() {
	r.Lock()
	defer r.Unlock()
	if r.hash == r.initialHash {
		return
	}

	// Make sure the IPFS MFS was not modified concurrently in the background
	stat, err := r.api.FilesStat("/")
	if err != nil {
		panic(err)
	}

	// List differences before and after rclone operations
	diff, err := r.api.ObjectDiff(r.initialHash, r.hash)
	if err != nil {
		panic(err)
	}

	if stat.Hash != r.initialHash {
		externalDiff, err := r.api.ObjectDiff(stat.Hash, r.initialHash)
		if err != nil {
			panic(err)
		}

		listChangedPath := func(changes []api.ObjectChange) (paths []string) {
			for _, change := range changes {
				paths = append(paths, change.Path)
			}
			return paths
		}
		externalChangedPaths := listChangedPath(externalDiff.Changes)
		localChangedPaths := listChangedPath(diff.Changes)

		for _, externalChangedPath := range externalChangedPaths {
			for _, localChangedPath := range localChangedPaths {
				if strings.HasPrefix(externalChangedPath, localChangedPath) ||
					strings.HasPrefix(localChangedPath, externalChangedPath) {
					panic("Error: concurrent modification of the IPFS MFS. Consistency not guaranteed.")
				}
			}
		}
	}

	// Persist changes to IPFS MFS
	for _, change := range diff.Changes {
		if change.Before != nil {
			err := r.api.FilesRm("/" + change.Path)
			if err != nil {
				panic(err)
			}
		}
		if change.After != nil {
			absolutePath := "/ipfs/" + change.After["/"]
			err := r.api.FilesCp(absolutePath, "/"+change.Path)
			if err != nil {
				panic(err)
			}
		}
	}
	r.log("Persisted root hash %s in IPFS MFS", r.hash)

	stat, err = r.api.FilesStat("/")
	if err != nil {
		panic(err)
	}
	r.initialHash = stat.Hash
	r.hash = stat.Hash
}

func (r *Root) periodicPersist() {
	nextTime := time.Now().Add(PersistPeriod)
	time.Sleep(time.Until(nextTime))
	r.persistToMFS()
	go r.periodicPersist()
}

var (
	DefaultTime   = time.Unix(0, 0)
	PersistPeriod = time.Second
	sharedRoot    = &SharedRoots{cache: make(map[string]*Root)}
)

// ------------------------------------------------------------

// Options defines the configuration for this backend
type Options struct {
	Endpoint string `config:"url"`
}

// Fs stores the interface to the remote HTTP files
type Fs struct {
	name          string
	root          string
	features      *fs.Features // optional features
	opt           Options      // options for this backend
	api           *api.Api     // the connection to the server
	ipfsRoot      *Root
	_emptyDirHash string // IPFS hash of an empty dir
}

// Object is a remote object that has been stat'd (so it exists, but is not necessarily open for reading)
type Object struct {
	fs       *Fs
	remote   string
	size     int64
	ipfsHash string
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
		CanHaveEmptyDirectories: true,
	}).Fill(f)

	ipfsPath := "/" // for IPFS MFS (will change for IPNS)

	sharedRoot.Lock()
	ipfsRootKey := opt.Endpoint + ":" + ipfsPath
	ipfsRoot := sharedRoot.cache[ipfsRootKey]
	if ipfsRoot == nil {
		ipfsRoot, err = NewRoot(f, ipfsPath)
		if err != nil {
			return nil, err
		}
		sharedRoot.cache[ipfsRootKey] = ipfsRoot
	}
	f.ipfsRoot = ipfsRoot
	sharedRoot.Unlock()

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

// Check if IPFS remote is a file (file type can only be obtained by
// listing files of the parent directory)
func (f *Fs) isFile(remote string) (error, bool) {
	f.ipfsRoot.RLock()
	rootHash := f.ipfsRoot.hash
	f.ipfsRoot.RUnlock()
	absolutePath := path.Join(rootHash, f.relativePath(remote))
	dir, file := path.Split(absolutePath)
	if dir == rootHash {
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
func convertSmallFileSize(cumulativeSize int64) int64 {
	switch {
	case cumulativeSize == 0:
		return 0
	case cumulativeSize < 9:
		return cumulativeSize - 6
	case cumulativeSize < 131:
		return cumulativeSize - 8
	case cumulativeSize < 139:
		return cumulativeSize - 9
	case cumulativeSize < 16388:
		return cumulativeSize - 11
	case cumulativeSize < 16398:
		return cumulativeSize - 12
	default:
		return cumulativeSize - 14
	}
}

// Convert IPFS object size to actual file size
func (f *Fs) convertToFileSize(objectStat api.ObjectStat) int64 {
	// Calculate file size
	var fileSize int64
	cumulativeSize := objectStat.CumulativeSize
	if cumulativeSize < (MaxChunkSize + 123) {
		// Single chunk file
		fileSize = convertSmallFileSize(cumulativeSize)
	} else {
		// Multiple chunk file
		i := cumulativeSize - objectStat.BlockSize
		maxSizeChunks := i / (MaxChunkSize + 14)
		remainingSizeChunk := i % (MaxChunkSize + 14)
		fileSize = i - (maxSizeChunks * 14) - (remainingSizeChunk - convertSmallFileSize(remainingSizeChunk))
	}
	return fileSize
}

// Name of the remote (as passed into NewFs)
func (f *Fs) Name() string {
	return f.name
}

// SharedRoots of the remote (as passed into NewFs)
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
	f.ipfsRoot.RLock()
	rootHash := f.ipfsRoot.hash
	f.ipfsRoot.RUnlock()
	absolutePath := path.Join(rootHash, f.relativePath(dir))
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
			stat, err := f.api.ObjectStat(path.Join(rootHash, f.relativePath(remote)))
			if err != nil {
				return nil, err
			}
			o := newObject(f, remote, stat)
			entries = append(entries, o)
		}
	}
	return entries, nil
}

func newObject(f *Fs, remote string, stat *api.ObjectStat) *Object {
	return &Object{
		fs:       f,
		remote:   remote,
		size:     f.convertToFileSize(*stat),
		ipfsHash: stat.Hash,
	}
}

// NewObject finds the Object at remote.  If it can't be found
// it returns the error fs.ErrorObjectNotFound. If is a directory
func (f *Fs) NewObject(remote string) (fs.Object, error) {
	f.ipfsRoot.RLock()
	absolutePath := path.Join(f.ipfsRoot.hash, f.relativePath(remote))
	f.ipfsRoot.RUnlock()
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
	o := newObject(f, remote, stat)
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

	f.ipfsRoot.Lock()
	result, err := f.api.ObjectPatchAddLink(f.ipfsRoot.hash, objectPath, fileAdded.Hash)
	if err != nil {
		f.ipfsRoot.Unlock()
		return nil, err
	}
	f.ipfsRoot.hash = result.Hash
	f.ipfsRoot.Unlock()
	return f.NewObject(src.Remote())
}

// Mkdir creates the container if it doesn't exist
func (f *Fs) Mkdir(dir string) error {
	emptyDirHash, err := f.emptyDirHash()
	if err != nil {
		return err
	}

	f.ipfsRoot.Lock()
	dirPath := f.relativePath(dir)
	result, err := f.api.ObjectPatchAddLink(f.ipfsRoot.hash, dirPath, emptyDirHash)
	if err != nil {
		f.ipfsRoot.Unlock()
		return err
	}
	f.ipfsRoot.hash = result.Hash
	f.ipfsRoot.Unlock()
	return nil
}

// Rmdir deletes the root folder
//
// Returns ErrorDirectoryNotEmpty if it isn't empty
func (f *Fs) Rmdir(dir string) error {
	f.ipfsRoot.Lock()
	absolutePath := path.Join(f.ipfsRoot.hash, f.relativePath(dir))
	stat, err := f.api.ObjectStat(absolutePath)
	if err != nil {
		f.ipfsRoot.Unlock()
		if _, ok := err.(*api.Error); ok {
			return fs.ErrorDirNotFound
		}
		return err
	}
	// Should not have children
	if stat.NumLinks > 0 {
		f.ipfsRoot.Unlock()
		return fs.ErrorDirectoryNotEmpty
	}

	dirPath := f.relativePath(dir)
	result, err := f.api.ObjectPatchRmLink(f.ipfsRoot.hash, dirPath)
	if err != nil {
		f.ipfsRoot.Unlock()
		return err
	}
	f.ipfsRoot.hash = result.Hash
	f.ipfsRoot.Unlock()
	return nil
}

func (f *Fs) Copy(src fs.Object, remote string) (fs.Object, error) {
	objectPath := f.relativePath(remote)
	var ipfsObject = src.(*Object)
	f.ipfsRoot.Lock()
	result, err := f.api.ObjectPatchAddLink(f.ipfsRoot.hash, objectPath, ipfsObject.ipfsHash)
	if err != nil {
		sharedRoot.Unlock()
		return nil, err
	}
	f.ipfsRoot.hash = result.Hash
	f.ipfsRoot.Unlock()
	return f.NewObject(remote)
}

func (f *Fs) Move(src fs.Object, remote string) (o fs.Object, err error) {
	if o, err = f.Copy(src, remote); err != nil {
		return nil, err
	}
	if err = src.Remove(); err != nil {
		return nil, err
	}
	return o, nil
}

func (f *Fs) DirMove(src fs.Fs, srcRemote string, dstRemote string) error {
	srcFs := src.(*Fs)
	f.ipfsRoot.Lock()
	defer f.ipfsRoot.Unlock()

	// Check dest dir does not exist
	dstRelativePath := f.relativePath(dstRemote)
	destAbsolutePath := path.Join(f.ipfsRoot.hash, dstRelativePath)
	destStat, err := f.api.ObjectStat(destAbsolutePath)
	if destStat != nil {
		return fs.ErrorDirExists
	}

	// Fetch source dir stats (for the hash)
	srcRelativePath := srcFs.relativePath(srcRemote)
	println("DIR MOVE", srcRelativePath, dstRelativePath)
	srcAbsolutePath := path.Join(f.ipfsRoot.hash, srcRelativePath)
	srcStat, err := f.api.ObjectStat(srcAbsolutePath)
	if err != nil {
		return err
	}

	// Copy dir by hash
	result, err := f.api.ObjectPatchAddLink(f.ipfsRoot.hash, dstRelativePath, srcStat.Hash)
	if err != nil {
		return err
	}
	f.ipfsRoot.hash = result.Hash

	// Remove original dir
	result, err = srcFs.api.ObjectPatchRmLink(f.ipfsRoot.hash, srcRelativePath)
	if err != nil {
		return err
	}
	f.ipfsRoot.hash = result.Hash
	return nil
}

func (f *Fs) MergeDirs(dirs []fs.Directory) error {
	firstDirectory := dirs[0]
	srcPath := f.relativePath(firstDirectory.Remote())

	f.ipfsRoot.Lock()
	defer f.ipfsRoot.Unlock()
	workingRootHash := f.ipfsRoot.hash
	for _, dir := range dirs[1:] {
		absolutePath := path.Join(workingRootHash, f.root, dir.Remote())
		links, err := f.api.Ls(absolutePath)
		if err != nil {
			return err
		}

		for _, link := range links {
			relativePath := path.Join(srcPath, link.Name)
			result, err := f.api.ObjectPatchAddLink(workingRootHash, relativePath, link.Hash)
			if err != nil {
				return err
			}
			workingRootHash = result.Hash
		}

		result, err := f.api.ObjectPatchRmLink(workingRootHash, f.relativePath(dir.Remote()))
		if err != nil {
			return err
		}
		workingRootHash = result.Hash
	}
	f.ipfsRoot.hash = workingRootHash
	return nil
}

func (f *Fs) PutStream(in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	return f.Put(in, src, options...)
}

func (f *Fs) Purge() error {
	f.ipfsRoot.Lock()
	defer f.ipfsRoot.Unlock()
	if f.root == "" {
		emptyDirHash, err := f.emptyDirHash()
		if err != nil {
			return err
		}
		f.ipfsRoot.hash = emptyDirHash
	} else {
		result, err := f.api.ObjectPatchRmLink(f.ipfsRoot.hash, f.root)
		if err != nil {
			return err
		}
		f.ipfsRoot.hash = result.Hash
	}
	return nil
}

func (f *Fs) PublicLink(remote string) (string, error) {
	f.ipfsRoot.RLock()
	absolutePath := path.Join(f.ipfsRoot.hash, f.relativePath(remote))
	f.ipfsRoot.RUnlock()

	if _, err := f.api.ObjectStat(absolutePath); err != nil {
		return "", err
	}
	return HttpGateway + absolutePath, nil
}

// ------------------------------------------------------------

// Fs returns the parent Fs²²
func (o *Object) Fs() fs.Info {
	return o.fs
}

func (o *Object) relativePath() string {
	return o.fs.relativePath(o.Remote())
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
	o.fs.ipfsRoot.RLock()
	absolutePath := path.Join(o.fs.ipfsRoot.hash, o.relativePath())
	o.fs.ipfsRoot.RUnlock()
	return o.fs.api.Cat(absolutePath, o.Size(), options...)
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
	o.fs.ipfsRoot.Lock()
	result, err := o.fs.api.ObjectPatchRmLink(o.fs.ipfsRoot.hash, o.relativePath())
	if err != nil {
		o.fs.ipfsRoot.Unlock()
		if _, ok := err.(*api.Error); ok {
			return fs.ErrorObjectNotFound
		}
		return err
	}
	o.fs.ipfsRoot.hash = result.Hash
	o.fs.ipfsRoot.Unlock()
	return nil
}

func (o *Object) ID() string {
	return o.ipfsHash
}

// Check the interfaces are satisfied
var (
	_ fs.Fs     = (*Fs)(nil)
	_ fs.Object = (*Object)(nil)

	// Optional Fs
	_ fs.Copier       = (*Fs)(nil)
	_ fs.Mover        = (*Fs)(nil)
	_ fs.PublicLinker = (*Fs)(nil)
	_ fs.Purger       = (*Fs)(nil)
	_ fs.PutStreamer  = (*Fs)(nil)
	_ fs.MergeDirser  = (*Fs)(nil)
	_ fs.DirMover     = (*Fs)(nil)

	// Optional Object
	_ fs.IDer = (*Object)(nil)
)
