package ipfs

import (
	"fmt"
	"github.com/ncw/rclone/backend/ipfs/api"
	"github.com/ncw/rclone/fs"
	"github.com/ncw/rclone/fs/config/configmap"
	"github.com/ncw/rclone/fs/config/configstruct"
	"github.com/ncw/rclone/fs/fshttp"
	"github.com/ncw/rclone/fs/hash"
	"github.com/ncw/rclone/lib/rest"
	"github.com/pkg/errors"
	"io"
	"net/http"
	"net/url"
	"path"
	"time"
)

// TODO: configure integration tests
// TODO: implement optional Fs interfaces
// TODO: implement IPFS hash
// TODO: implement read access to `/ipfs/<HASH>` paths
// TODO: implement read/write access to `/ipns/<HASH>` paths

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

// NewFs creates a new Fs object from the name and root. It connects to
// the host specified in the config file.
func NewFs(name, root string, m configmap.Mapper) (fs.Fs, error) {
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
		srv:  rest.NewClient(client).SetRoot(opt.Endpoint),
	}
	f.features = (&fs.Features{
		CaseInsensitive:         true,
		CanHaveEmptyDirectories: true,
	}).Fill(f)
	f.srv.SetErrorHandler(errorHandler)

	return f, nil
}

// errorHandler parses a non 2xx error response into an error
func errorHandler(resp *http.Response) error {
	// Decode error response
	errResponse := new(api.Error)
	err := rest.DecodeJSON(resp, &errResponse)
	if err != nil {
		fs.Debugf(nil, "Couldn't decode error response: %v", err)
	}
	return errResponse
}

// Options defines the configuration for this backend
type Options struct {
	Endpoint string `config:"url"`
}

// Fs stores the interface to the remote HTTP files
type Fs struct {
	name        string
	root        string
	features    *fs.Features // optional features
	opt         Options      // options for this backend
	endpoint    *url.URL
	endpointURL string       // endpoint as a string
	srv         *rest.Client // the connection to the server
}

// Object is a remote object that has been stat'd (so it exists, but is not necessarily open for reading)
type Object struct {
	fs      *Fs
	remote  string
	size    int64
	modTime time.Time
}

// ------------------------------------------------------------

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
	result, err := api.FilesList(f.srv, path.Join(f.root, dir))
	if err != nil {
		if apiError, ok := err.(*api.Error); ok && apiError.Message == "file does not exist" {
			return nil, fs.ErrorDirNotFound
		}
		return nil, err
	}

	for _, entry := range result.Entries {
		remote := path.Join(dir, entry.Name)
		modTime := time.Unix(0, 0)

		if entry.Type == api.FileEntryTypeFolder {
			d := fs.NewDir(remote, modTime)
			entries = append(entries, d)
		} else {
			o := &Object{
				fs:      f,
				remote:  remote,
				modTime: modTime,
				size:    int64(entry.Size),
			}
			entries = append(entries, o)
		}
	}

	return entries, nil
}

// NewObject finds the Object at remote.  If it can't be found
// it returns the error fs.ErrorObjectNotFound.
func (f *Fs) NewObject(remote string) (fs.Object, error) {
	stat, err := api.FilesStat(f.srv, path.Join(f.root, remote))
	if err != nil {
		if apiError, ok := err.(*api.Error); ok && apiError.Message == "file does not exist" {
			return nil, fs.ErrorObjectNotFound
		}
		return nil, err
	}
	o := &Object{
		fs:     f,
		remote: remote,
		size:   int64(stat.Size),
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
	fileAdded, err := api.Add(f.srv, in, file)
	if err != nil {
		return nil, err
	}
	err = api.FilesCp(f.srv, "/ipfs/"+fileAdded.Hash, src.Remote())
	if err != nil {
		return nil, err
	}
	o, err := f.NewObject(fileAdded.Name)
	if err != nil {
		return nil, err
	}
	return o, nil
}

// Mkdir creates the container if it doesn't exist
func (f *Fs) Mkdir(dir string) error {
	return api.FilesMkdir(f.srv, dir)
}

// Rmdir deletes the root folder (emulated with files/rm, files/stat and files/ls)
//
// Returns ErrorDirectoryNotEmpty if it isn't empty
func (f *Fs) Rmdir(dir string) error {
	dirPath := path.Join(f.root, dir)

	stat, err := api.FilesStat(f.srv, dirPath)
	if err != nil {
		if apiError, ok := err.(*api.Error); ok && apiError.Message == "file does not exist" {
			return fs.ErrorDirNotFound
		}
		return err
	}
	// Should not be a file
	if stat.Type == api.FileStatTypeFile {
		return fs.ErrorIsFile
	}

	list, err := f.List(dir)
	if err != nil {
		return err
	}
	// Should not have children
	if list.Len() > 0 {
		return fs.ErrorDirectoryNotEmpty
	}

	err = api.FilesRm(f.srv, dirPath)
	if err != nil {
		return err
	}
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
	return errors.New("ipfs remote can't get or set modification time")
}

// Storable returns a boolean showing whether this object storable
func (o *Object) Storable() bool {
	return true
}

// Open an object for read
func (o *Object) Open(options ...fs.OpenOption) (in io.ReadCloser, err error) {
	panic("implement me")
}

// Update the object with the contents of the io.Reader, modTime and size
//
// If existing is set then it updates the object rather than creating a new one
//
// The new object may have been created if an error is returned
func (o *Object) Update(in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	return api.FilesWrite(o.fs.srv, o.remote, in)
}

// Remove an object
func (o *Object) Remove() error {
	return api.FilesRm(o.fs.srv, o.remote)
}

// Check the interfaces are satisfied
var (
	_ fs.Fs     = &Fs{}
	_ fs.Object = &Object{}
)
