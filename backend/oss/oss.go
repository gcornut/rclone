package oss

/* FIXME

- needs pacer
- just read f.c.Bucket once? if it is safe for concurrent reads
- other optional methods?
- fix ERROR : : Entry doesn't belong in directory "" (same as directory) - ignoring in sync tests
- chunk tester?
- multipart uploads for files bigger than 5 GB
*/

import (
	"fmt"
	"io"
	"net/http"
	"path"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/ncw/rclone/fs"
	"github.com/ncw/rclone/fs/config/configmap"
	"github.com/ncw/rclone/fs/config/configstruct"
	"github.com/ncw/rclone/fs/fshttp"
	"github.com/ncw/rclone/fs/hash"
	"github.com/ncw/rclone/fs/walk"
	"github.com/pkg/errors"
)

// Register with Fs
func init() {
	fs.Register(&fs.RegInfo{
		Name:        "oss",
		Description: "Alibaba Cloud Object Storage Service",
		NewFs:       NewFs,
		Options: []fs.Option{{
			Name: "env_auth",
			Help: "Get oss credentials from runtime (environment variables). Only applies if access_id and access_key is blank.",
			Examples: []fs.OptionExample{
				{
					Value: "false",
					Help:  "Enter oss credentials in the next step",
				}, {
					Value: "true",
					Help:  "Get oss credentials from the environment (env vars or IAM)",
				},
			},
		}, {
			Name: "access_id",
			Help: "oss Access Key ID - leave blank for anonymous access or runtime credentials.",
		}, {
			Name: "access_key",
			Help: "oss Secret Access Key (password) - leave blank for anonymous access or runtime credentials.",
		}, {
			Name: "region",
			Help: "Region to connect to.",
			Examples: []fs.OptionExample{{
				Value: "oss-cn-hangzhou",
				Help:  "China east (hangzhou) The default endpoint - a good choice if you are unsure.",
			}, {
				Value: "oss-cn-shanghai",
				Help:  "China east (shanghai) Region\nNeeds location constraint cn-east-2.",
			}, {
				Value: "oss-cn-qingdao",
				Help:  "China north (qingdao) Region\nNeeds location constraint cn-north-1.",
			}, {
				Value: "oss-cn-beijing",
				Help:  "China north (beijing) Region\nNeeds location constraint cn-north-2.",
			}, {
				Value: "oss-cn-zhangjiakou",
				Help:  "China north (zhangjiakou) Region\nNeeds location constraint cn-north-3.",
			}, {
				Value: "oss-cn-shenzhen",
				Help:  "China south (shenzhen) Region\nNeeds location constraint cn-south-1.",
			}, {
				Value: "oss-cn-hongkong",
				Help:  "China (hongkong) Region\nNeeds location constraint cn-hongkong.",
			}, {
				Value: "oss-us-west-1",
				Help:  "west America (Silicon Valley)\nNeeds location constraint us-west-1.",
			}, {
				Value: "oss-us-east-1",
				Help:  "east America (Virginia) Region\nNeeds location constraint us-east-1.",
			}, {
				Value: "oss-ap-southeast-1",
				Help:  "Asia Pacific (Singapore) Region\nNeeds location constraint ap-southeast-1.",
			}, {
				Value: "oss-ap-southeast-2",
				Help:  "Asia Pacific (Sydney) Region\nNeeds location constraint ap-southeast-2.",
			}, {
				Value: "oss-ap-northeast-1",
				Help:  "Asia Pacific (Japanese)\nNeeds location constraint ap-northeast-1.",
			}, {
				Value: "oss-eu-central-1",
				Help:  "Central Europe (Frankfurt)\nNeeds location constraint eu-central-1.",
			}, {
				Value: "oss-me-east-1",
				Help:  "Middle East (Dubai) Region\nNeeds location constraint me-east-1.",
			}},
		}, {
			// Enter the endpoint
			// oss endpoints: https://help.aliyun.com/document_detail/31837.html
			Name: "endpoint",
			Help: "Endpoint for OSS API.\nLeave blank if using OSS to use the default endpoint for the region.\nSpecify if using an OSS clone such as Ceph.",
		}, {
			Name: "location_constraint",
			Help: "Location constraint - must be set to match the Region. Used when creating buckets only.",
			Examples: []fs.OptionExample{{
				Value: "cn-hangzhou",
				Help:  "China east ( hangzhou) Region.",
			}, {
				Value: "cn-shanghai",
				Help:  "China east (shanghai) Region.",
			}, {
				Value: "cn-qingdao",
				Help:  "China north (qingdao) Region.",
			}, {
				Value: "cn-beijing",
				Help:  "China north (beijing) Region.",
			}, {
				Value: "cn-zhangjiakou",
				Help:  "China north (zhangjiakou) Region.",
			}, {
				Value: "cn-shenzhen",
				Help:  "China south (shenzhen) Region.",
			}, {
				Value: "cn-hongkong",
				Help:  "China (hongkong) Region.",
			}, {
				Value: "us-west-1",
				Help:  "west America (Silicon Valley).",
			}, {
				Value: "us-east-1",
				Help:  "east America (Virginia) Region.",
			}, {
				Value: "ap-southeast-1",
				Help:  "Asia Pacific (Singapore) Region.",
			}, {
				Value: "ap-southeast-2",
				Help:  "Asia Pacific (Sydney) Region.",
			}, {
				Value: "ap-northeast-1",
				Help:  "Asia Pacific (Japanese).",
			}, {
				Value: "eu-central-1",
				Help:  "Central Europe (Frankfurt).",
			}, {
				Value: "me-east-1",
				Help:  "Middle East (Dubai) Region.",
			}},
		}, {
			Name: "acl",
			Help: "Canned ACL used when creating buckets adding/or storing objects in oss.",
			Examples: []fs.OptionExample{{
				Value: "private",
				Help:  "Owner gets FULL_CONTROL. No one else has access rights (default).",
			}, {
				Value: "public-read",
				Help:  "Owner gets FULL_CONTROL. The AllUsers group gets READ access.",
			}, {
				Value: "public-read-write",
				Help:  "Owner gets FULL_CONTROL. The AllUsers group gets READ and WRITE access.\nGranting this on a bucket is generally not recommended.",
			}, {
				Value: "default",
				Help:  "Owner gets FULL_CONTROL.the bucket is not recommended.",
			}},
		}, {
			Name: "storage_class",
			Help: "the StorageClassType of the Bucket",
			Examples: []fs.OptionExample{{
				Value: "Standard",
				Help:  "StorageStandard，Standard storage mode",
			}, {
				Value: "Archive",
				Help:  "StorageArchive,Archive storage mode.",
			}, {
				Value: "IA",
				Help:  "StorageIA,Low frequency storage mode.",
			}},
		}},
	})
}

// Options defines the configuration for this backend
type Options struct {
	EnvAuth            bool   `config:"env_auth"`
	AccessID           string `config:"access_id"`
	AccessKey          string `config:"access_key"`
	Region             string `config:"region"`
	Endpoint           string `config:"endpoint"`
	LocationConstraint string `config:"location_constraint"`
	ACL                string `config:"acl"`
	StorageClass       string `config:"storage_class"`
}

// Constants
const (
	metaMtime      = "Mtime" // the meta key to store mtime
	timeFormatIn   = time.RFC3339
	timeFormatOut  = "2006-01-02T15:04:05.000000000Z07:00"
	listChunkSize  = 1000                   // number of items to read at once
	maxRetries     = 10                     // The maximum number of retries for each operation to be performed
	maxSizeForCopy = 5 * 1024 * 1024 * 1024 // The maximum size of object we can COPY
)

// Fs represents a remote oss server
type Fs struct {
	name               string       // the name of the remote
	root               string       // the root path of the bucket - ignores all objects that are above it
	features           *fs.Features // optional features
	c                  *oss.Client  // the connection to the oss server
	bucket             string       // the bucket we are working on
	bucketOKMu         sync.Mutex   // mutex to protect bucket OK
	bucketOK           bool         // true if we have created the bucket
	locationConstraint string       // location constraint of new buckets
	storageClass       string       // storage class
	acl                string       // bucket acl
}

// Object describes a oss object
type Object struct {
	// Will definitely have everything but meta which may be nil
	//
	// List will read everything but meta - to fill that in you
	// need to call readMetaData
	fs           *Fs               // what this object is part of
	remote       string            // The remote path
	meta         map[string]string // The object metadata if known - may be nil
	key          string            // the key of the object
	mimeType     string            // the mimeType of the object
	size         int64             // the size of the object
	etag         string            // the etag of the object
	lastModified time.Time         // the lastModified of the object
}

// Name of the remote (as passed into NewFs)
func (f *Fs) Name() string {
	return f.name
}

// Root of the remote (as passed into NewFs)
func (f *Fs) Root() string {
	if f.root == "" {
		return f.bucket
	}
	return f.bucket + "/" + f.root
}

// String converts this Fs to a string
func (f *Fs) String() string {
	if f.root == "" {
		return fmt.Sprintf("oss bucket %s", f.bucket)
	}
	return fmt.Sprintf("oss bucket %s path %s", f.bucket, f.root)
}

// Features returns the optional features of this Fs
func (f *Fs) Features() *fs.Features {
	return f.features
}

// Pattern to match a oss path
var matcher = regexp.MustCompile(`^([^/]*)(.*)$`)

// parseParse parses a oss 'url'
func ossParsePath(path string) (bucket, directory string, err error) {
	parts := matcher.FindStringSubmatch(path)
	if parts == nil {
		err = errors.Errorf("couldn't parse bucket out of oss path %q", path)
	} else {
		bucket, directory = parts[1], parts[2]
		// Remove both sides of "/"
		directory = strings.Trim(directory, "/")
	}
	return
}

// ossConnection makes a connection to oss
func ossConnection(endpoint string, accessID string, accessKey string) (*oss.Client, error) {
	client, err := oss.New(endpoint, accessID, accessKey,
		oss.HTTPClient(fshttp.NewClient(fs.Config)),
	)
	return client, err
}

// NewFs constructs an Fs from the path, bucket:path
func NewFs(name, root string, m configmap.Mapper) (fs.Fs, error) {
	// Parse config into Options struct
	opt := new(Options)
	err := configstruct.Set(m, opt)
	if err != nil {
		return nil, err
	}
	bucket, directory, err := ossParsePath(root)
	if err != nil {
		return nil, err
	}
	//Verify the account
	c, err := ossConnection(opt.Endpoint, opt.AccessID, opt.AccessKey)
	if err != nil {
		return nil, err
	}
	f := &Fs{
		name:               name, //name of the remote
		c:                  c,
		bucket:             bucket,    //name of the bucket
		root:               directory, //directory in the bucket,may be nil
		acl:                opt.ACL,
		locationConstraint: opt.LocationConstraint,
		storageClass:       opt.StorageClass,
	}
	f.features = (&fs.Features{
		ReadMimeType:  true,
		WriteMimeType: true,
		BucketBased:   true,
	}).Fill(f)

	if f.root != "" {
		// Check to see if the object exists
		bucket, _ := f.c.Bucket(f.bucket)
		objectExists, err := bucket.IsObjectExist(f.root)
		if err == nil && objectExists {
			f.root = path.Dir(directory)
			if f.root == "." {
				f.root = ""
			} else {
				f.root += "/"
			}
			// return an error with an fs which points to the parent
			return f, fs.ErrorIsFile
		}
		f.root += "/"
	}
	return f, nil
}

// Return an Object from a path
// If it can't be found it returns the error ErrorObjectNotFound.
func (f *Fs) newObjectWithInfo(remote string, info *oss.ObjectProperties) (fs.Object, error) {
	o := &Object{
		fs:     f,
		remote: remote,
	}
	if info != nil {
		err := o.decodeMetaData(info)
		if err != nil {
			return nil, err
		}
	} else {
		err := o.readMetaData(f) // reads info and meta, returning an error
		if err != nil {
			return nil, err
		}
	}
	return o, nil
}

// NewObject finds the Object at remote.  If it can't be found
// it returns the error fs.ErrorObjectNotFound.
func (f *Fs) NewObject(remote string) (fs.Object, error) {
	return f.newObjectWithInfo(remote, nil)
}

// listFn is called from list to handle an object.
type listFn func(remote string, object *oss.ObjectProperties, isDirectory bool) error

// list the objects into the function supplied
//
// dir is the starting directory, "" for root
//
// Set recurse to read sub directories
func (f *Fs) list(dir string, recurse bool, fn listFn) error {
	root := f.root
	if dir != "" {
		root += dir + "/"
	}
	delimiter := oss.Delimiter("")
	if !recurse {
		delimiter = oss.Delimiter("/")
	}
	maxKeys := oss.MaxKeys(1000)
	var marker oss.Option
	pre := oss.Prefix(root)
	bucket, _ := f.c.Bucket(f.bucket)
	for {
		listObjects, err := bucket.ListObjects(pre, delimiter, maxKeys, marker)
		if err != nil {
			if ossErr, ok := err.(oss.ServiceError); ok {
				if ossErr.StatusCode == http.StatusNotFound {
					err = fs.ErrorDirNotFound
				}
			}
			return err
		}
		rootLength := len(f.root)
		if !recurse {
			for _, commonPrefix := range listObjects.CommonPrefixes {
				if commonPrefix == "" {
					fs.Logf(f, "Nil common prefix received")
					continue
				}
				remote := commonPrefix
				if !strings.HasPrefix(remote, f.root) {
					fs.Logf(f, "Odd name received %q", remote)
					continue
				}
				remote = remote[rootLength:]
				if strings.HasSuffix(remote, "/") {
					remote = remote[:len(remote)-1]
				}
				err = fn(remote, &oss.ObjectProperties{Key: remote}, true)
				if err != nil {
					return err
				}
			}
		}
		for _, object := range listObjects.Objects {
			key := object.Key
			if !strings.HasPrefix(key, f.root) {
				fs.Logf(f, "Odd name received %q", key)
				continue
			}
			if strings.HasSuffix(key, "/") {
				continue
			}
			remote := key[rootLength:]
			err = fn(remote, &object, false)
			if err != nil {
				return err
			}
		}
		if !listObjects.IsTruncated {
			break
		}
		marker = oss.Marker(listObjects.NextMarker)
	}

	return nil
}

// Convert a list item into a DirEntry
func (f *Fs) itemToDirEntry(remote string, object *oss.ObjectProperties, isDirectory bool) (fs.DirEntry, error) {
	if isDirectory {
		size := int64(0)
		if &object.Size != nil {
			size = object.Size
		}
		d := fs.NewDir(remote, time.Time{}).SetSize(size)
		return d, nil
	}
	o, err := f.newObjectWithInfo(remote, object)
	if err != nil {
		return nil, err
	}
	return o, nil
}

// listDir returns directory content.
func (f *Fs) listDir(dir string) (entries fs.DirEntries, err error) {
	// List the objects and directories
	err = f.list(dir, false, func(remote string, object *oss.ObjectProperties, isDirectory bool) error {
		if strings.HasSuffix(remote, "/") {
			return nil
		}
		entry, err := f.itemToDirEntry(remote, object, isDirectory)
		if err != nil {
			return err
		}
		if entry != nil {
			entries = append(entries, entry)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return entries, nil
}

// listBuckets returns buckets list.
func (f *Fs) listBuckets(dir string) (entries fs.DirEntries, err error) {
	if dir != "" {
		return nil, fs.ErrorListBucketRequired
	}
	listBucketsResult, err := f.c.ListBuckets(nil)
	if err != nil {
		return nil, err
	}
	for _, bucket := range listBucketsResult.Buckets {
		d := fs.NewDir(bucket.Name, bucket.CreationDate)
		entries = append(entries, d)
	}
	return entries, nil
}

// List the objects and directories in dir into entries.  The
// entries can be returned in any order but should be for a
// complete directory.
//
// dir should be "" to list the root, and should not have
// trailing slashes.
//
// This should return ErrDirNotFound if the directory isn't
// found.
func (f *Fs) List(dir string) (entries fs.DirEntries, err error) {
	if f.bucket == "" {
		return f.listBuckets(dir)
	}
	return f.listDir(dir)
}

// ListR lists the objects and directories of the Fs starting
// from dir recursively into out.
//
// dir should be "" to start from the root, and should not
// have trailing slashes.
//
// This should return ErrDirNotFound if the directory isn't
// found.
//
// It should call callback for each tranche of entries read.
// These need not be returned in any particular order.  If
// callback returns an error then the listing will stop
// immediately.
//
// Don't implement this unless you have a more efficient way
// of listing recursively that doing a directory traversal.
func (f *Fs) ListR(dir string, callback fs.ListRCallback) (err error) {
	if f.bucket == "" {
		return fs.ErrorListBucketRequired
	}
	list := walk.NewListRHelper(callback)
	err = f.list(dir, true, func(remote string, object *oss.ObjectProperties, isDirectory bool) error {
		entry, err := f.itemToDirEntry(remote, object, isDirectory)
		if err != nil {
			return err
		}
		return list.Add(entry)
	})
	if err != nil {
		return err
	}
	return list.Flush()
}

// Put the Object into the bucket
func (f *Fs) Put(in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	// Temporary Object under construction
	fs := &Object{
		fs:     f,
		remote: src.Remote(),
	}
	return fs, fs.Update(in, src, options...)
}

// PutStream uploads to the remote path with the modTime given of indeterminate size
func (f *Fs) PutStream(in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	return f.Put(in, src, options...)
}

// NB this can return incorrect results if called immediately after bucket deletion
func (f *Fs) dirExists() (bool, error) {
	isBucketExist, err := oss.Client.IsBucketExist(*f.c, f.bucket)
	return isBucketExist, err
}

// Mkdir checks if directory exists otherwise it will create one. (container, bucket)
func (f *Fs) Mkdir(dir string) error {
	f.bucketOKMu.Lock()
	defer f.bucketOKMu.Unlock()
	if f.bucketOK {
		return nil
	}
	err := f.c.CreateBucket(f.bucket)
	if err != nil {
		if err.Error() == "BucketAlreadyOwnedByYou" {
			err = nil
		}
	}
	if err == nil {
		f.bucketOK = true
	}
	return err
}

// Rmdir removes the directory (container, bucket) if empty
// Return an error if it doesn't exist or isn't empty
func (f *Fs) Rmdir(dir string) error {
	f.bucketOKMu.Lock()
	defer f.bucketOKMu.Unlock()
	if f.root != "" || dir != "" {
		return nil
	}
	err := f.c.DeleteBucket(f.bucket)
	if err == nil {
		f.bucketOK = false
	}
	return err
}

// Precision of the remote
func (f *Fs) Precision() time.Duration {
	return time.Nanosecond
}

// Copy src to this remote using server side copy operations.
//
// This is stored with the remote path given
//
// It returns the destination Object and a possible error
//
// Will only be called if src.Fs().Name() == f.Name()
//
// If it isn't possible then return fs.ErrorCantCopy
func (f *Fs) Copy(src fs.Object, remote string) (fs.Object, error) {
	err := f.Mkdir("")
	if err != nil {
		return nil, err
	}
	srcObj, ok := src.(*Object)
	if !ok {
		fs.Debugf(src, "Can't copy - not same remote type")
		return nil, fs.ErrorCantCopy
	}
	if srcObj.size >= maxSizeForCopy {
		return nil, errors.Errorf("Copy is unsupported for objects bigger than %v bytes", fs.SizeSuffix(maxSizeForCopy))
	}
	srcFs := srcObj.fs
	err = srcObj.readMetaData(srcFs) // reads info and meta, returning an error
	if err != nil {
		return nil, err
	}
	key := f.root + remote
	sourceKey := srcFs.root + srcObj.remote
	ossOptions := []oss.Option{
		oss.ContentType(srcObj.mimeType),
		oss.MetadataDirective(oss.MetaReplace),
	}
	for k, v := range srcObj.meta {
		ossOptions = append(ossOptions, oss.Meta(k, v))
	}
	bucket, err := srcFs.c.Bucket(srcFs.bucket)
	if err != nil {
		return nil, err
	}
	_, err = bucket.CopyObjectTo(f.bucket, key, sourceKey, ossOptions...)
	if err != nil {
		return nil, err
	}
	return f.NewObject(remote)
}

// Hashes returns the supported hash sets.
func (f *Fs) Hashes() hash.Set {
	return hash.Set(hash.MD5)
}

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

var matchMd5 = regexp.MustCompile(`^[0-9a-f]{32}$`)

// Hash returns the Md5sum of an object returning a lowercase hex string
func (o *Object) Hash(t hash.Type) (string, error) {
	if t != hash.MD5 {
		return "", hash.ErrUnsupported
	}
	etag := strings.Trim(strings.ToLower(o.etag), `"`)
	// Check the etag is a valid md5sum
	if !matchMd5.MatchString(etag) {
		fs.Debugf(o, "Invalid md5sum (probably multipart uploaded) - ignoring: %q", etag)
		return "", nil
	}
	return etag, nil
}

// Size returns the size of an object in bytes
func (o *Object) Size() int64 {
	return o.size
}

// decodeMetaData sets the metadata in the object from an oss.ObjectProperties
//
// Sets
//  o.etag
//  o.size
//  o.lastModified
//  o.mimeType
//  o.meta = nil
func (o *Object) decodeMetaData(info *oss.ObjectProperties) (err error) {
	// Set info but not meta
	o.etag = info.ETag
	o.size = info.Size
	o.lastModified = info.LastModified
	o.mimeType = info.Type
	o.meta = nil
	return nil
}

// decodeMetaDataFromHeaders sets the metadata in the object from its headers
//
// Sets
//  o.etag
//  o.size
//  o.lastModified
//  o.mimeType
//  o.meta
func (o *Object) decodeMetaDataFromHeaders(meta http.Header) (err error) {
	o.etag = meta.Get("ETag")
	contentLength := meta.Get("Content-Length")
	size, err := strconv.ParseInt(contentLength, 10, 64)
	if err != nil {
		return errors.Wrapf(err, "decodeMetaDataFromHeaders failed to parse Content-Length %q", contentLength)
	}
	o.size = size
	lastModified := meta.Get("Last-Modified")
	if t, err := time.Parse(http.TimeFormat, lastModified); err == nil {
		o.lastModified = t
	} else {
		fs.Debugf(nil, "Couldn't parse lastmodified from %q: %v", lastModified, err)
	}
	o.mimeType = meta.Get("Content-Type")
	o.meta = make(map[string]string)
	for key, values := range meta {
		for _, value := range values {
			if strings.HasPrefix(key, oss.HTTPHeaderOssMetaPrefix) {
				key := key[len(oss.HTTPHeaderOssMetaPrefix):]
				o.meta[key] = value
			}
		}
	}
	return nil
}

// readMetaData gets the metadata if it hasn't already been fetched
func (o *Object) readMetaData(f *Fs) (err error) {
	if o.meta != nil {
		return nil
	}
	key := o.fs.root + o.remote
	archiveBucket, err := f.c.Bucket(o.fs.bucket)
	if err != nil {
		return err
	}
	meta, err := archiveBucket.GetObjectDetailedMeta(key)
	if err != nil {
		if ossErr, ok := err.(oss.ServiceError); ok {
			if ossErr.StatusCode == http.StatusNotFound {
				err = fs.ErrorObjectNotFound
			}
		}
		return err
	}
	return o.decodeMetaDataFromHeaders(meta)
}

// ModTime returns the modification time of the object
// It attempts to read the objects mtime and if that isn't present the
// LastModified returned in the http headers
func (o *Object) ModTime() time.Time {
	if fs.Config.UseServerModTime {
		return o.lastModified
	}
	err := o.readMetaData(o.fs)
	if err != nil {
		fs.Logf(o, "Failed to read metadata: %v", err)
		return time.Now()
	}
	// read mtime out of metadata if available
	d, ok := o.meta[metaMtime]
	if !ok {
		return o.lastModified
	}
	modTime, err := time.Parse(timeFormatIn, d)
	if err != nil {
		fs.Debugf(o, "Couldn't parse modification time from %q: %v", d, err)
		return o.lastModified
	}
	return modTime
}

// SetModTime sets the modification time of the local fs object
func (o *Object) SetModTime(modTime time.Time) error {
	err := o.readMetaData(o.fs)
	if err != nil {
		return err
	}
	if o.size >= maxSizeForCopy {
		fs.Debugf(o, "SetModTime is unsupported for objects bigger than %v bytes", fs.SizeSuffix(maxSizeForCopy))
		return nil
	}
	// Copy the object to itself to update the metadata
	key := o.fs.root + o.remote
	o.meta[metaMtime] = modTime.Format(timeFormatOut)
	ossOptions := []oss.Option{
		oss.ContentType(o.mimeType),
		oss.MetadataDirective(oss.MetaReplace),
	}
	for k, v := range o.meta {
		ossOptions = append(ossOptions, oss.Meta(k, v))
	}
	bucket, err := o.fs.c.Bucket(o.fs.bucket)
	if err != nil {
		return err
	}
	_, err = bucket.CopyObject(key, key, ossOptions...)
	return err
}

// Storable returns a boolean indicating if the object is storable.
func (o *Object) Storable() bool {
	return true
}

// Open an object for read
func (o *Object) Open(options ...fs.OpenOption) (in io.ReadCloser, err error) {
	key := o.fs.root + o.remote
	bucket, _ := o.fs.c.Bucket(o.fs.bucket)
	opts := []oss.Option{}
	for _, option := range options {
		switch x := option.(type) {
		case *fs.SeekOption:
			value := strconv.FormatInt(x.Offset, 10)
			opts = append(opts, oss.NormalizedRange(value+"-"))
		case *fs.RangeOption:
			value := ""
			if x.Start >= 0 {
				value += strconv.FormatInt(x.Start, 10)

			}
			value += "-"
			if x.End >= 0 && x.End <= o.size {
				value += strconv.FormatInt(x.End, 10)
			}
			opts = append(opts, oss.NormalizedRange(value))
		default:
			if option.Mandatory() {
				fs.Logf(o, "Unsupported mandatory option: %v", option)
			}
		}
	}
	resp, err := bucket.GetObject(key, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// Update the Object from in with modTime and size
func (o *Object) Update(in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	size := src.Size()
	if size >= maxSizeForCopy {
		fs.Debugf(o, "Can't upload files bigger than %v bytes yet", fs.SizeSuffix(maxSizeForCopy))
		return nil
	}
	err := o.fs.Mkdir("")
	if err != nil {
		return err
	}
	mimeType := fs.MimeType(src)
	bucket, err := o.fs.c.Bucket(o.fs.bucket)
	if err != nil {
		return err
	}
	key := o.fs.root + o.remote
	ossOptions := []oss.Option{oss.ContentType(mimeType)}
	for k, v := range o.meta {
		ossOptions = append(ossOptions, oss.Meta(k, v))
	}
	ossOptions = append(ossOptions, oss.Meta(metaMtime, src.ModTime().Format(timeFormatOut)))
	err = bucket.PutObject(key, in, ossOptions...)
	if err != nil {
		return err
	}
	// Read the metadata from the newly created object
	o.meta = nil // wipe old metadata
	return o.readMetaData(o.fs)
}

// Remove an object
func (o *Object) Remove() error {
	bucket, err := o.fs.c.Bucket(o.fs.bucket)
	if err != nil {
		return err
	}
	key := o.fs.root + o.remote
	isObjectExist, _ := bucket.IsObjectExist(key)
	if isObjectExist {
		erro := bucket.DeleteObject(key)
		return erro
	}
	return nil
}

// MimeType returns the content type of the Object if known, or "" if not
// MimeType of an Object if known, "" otherwise
func (o *Object) MimeType() string {
	err := o.readMetaData(o.fs)
	if err != nil {
		fs.Logf(o, "Failed to read metadata: %v", err)
		return ""
	}
	return o.mimeType
}

// Check if the interfaces are satisfied.
var (
	_ fs.Fs          = &Fs{}
	_ fs.Copier      = &Fs{}
	_ fs.PutStreamer = &Fs{}
	_ fs.ListRer     = &Fs{}
	_ fs.Object      = &Object{}
	_ fs.MimeTyper   = &Object{}
)
