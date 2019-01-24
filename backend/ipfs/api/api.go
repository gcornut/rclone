package api

import (
	"github.com/ncw/rclone/fs"
	"github.com/ncw/rclone/lib/rest"
	"io"
	"net/http"
	"net/url"
	"path"
)

// TODO: Add all of this in github.com/ipfs/go-ipfs-api (and add go-ipfs-api as a dependency for rclone)

type Api struct {
	srv *rest.Client // the connection to the server
}

func NewApi(client *http.Client, endpoint string) *Api {
	api := Api{
		srv: rest.NewClient(client).SetRoot(endpoint),
	}
	api.srv.SetErrorHandler(errorHandler)
	return &api
}

// errorHandler parses a non 2xx error response into an error
func errorHandler(resp *http.Response) error {
	// Decode error response
	errResponse := new(Error)
	err := rest.DecodeJSON(resp, &errResponse)
	if err != nil {
		fs.Debugf(nil, "Couldn't decode error response: %v", err)
	}
	return errResponse
}

// /api/v0/files/stat
func (a *Api) FilesStat(file string) (result *FileStat, err error) {
	opts := rest.Opts{
		Method: "GET",
		Path:   "/api/v0/files/stat",
		Parameters: url.Values{
			"arg": []string{file},
		},
	}
	_, err = a.srv.CallJSON(&opts, nil, &result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// /api/v0/files/ls
func (a *Api) FilesList(dir string) (fileEntries *FileList, err error) {
	opts := rest.Opts{
		Method: "GET",
		Path:   "/api/v0/files/ls",
		Parameters: url.Values{
			"arg": []string{dir},
			"l":   []string{"true"},
			"U":   []string{"true"},
		},
	}
	_, err = a.srv.CallJSON(&opts, nil, &fileEntries)
	if err != nil {
		return nil, err
	}
	return fileEntries, nil
}

// /api/v0/add
func (a *Api) Add(in io.Reader, name string) (result *FileAdded, err error) {
	opts := rest.Opts{
		Method:               "POST",
		Path:                 "/api/v0/add",
		MultipartParams:      url.Values{},
		MultipartContentName: "file",
		MultipartFileName:    name,
		Body:                 in,
		Parameters: url.Values{
			"pin": []string{"false"},
		},
	}
	_, err = a.srv.CallJSON(&opts, nil, &result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// /api/v0/files/write
func (a *Api) FilesWrite(file string, in io.Reader) error {
	_, fileName := path.Split(file)
	opts := rest.Opts{
		Method: "POST",
		Path:   "/api/v0/files/write",
		Parameters: url.Values{
			"arg":      []string{file},
			"truncate": []string{"true"},
		},
		MultipartParams:      url.Values{},
		MultipartContentName: "file",
		MultipartFileName:    fileName,
		Body:                 in,
	}
	_, err := a.srv.CallJSON(&opts, nil, nil)
	return err
}

// /api/v0/files/cp
func (a *Api) FilesCp(from string, to string) error {
	opts := rest.Opts{
		Method: "GET",
		Path:   "/api/v0/files/cp",
		Parameters: url.Values{
			"arg": []string{from, to},
		},
	}
	_, err := a.srv.Call(&opts)
	if err != nil {
		return err
	}
	return nil
}

// /api/v0/files/mkdir
func (a *Api) FilesMkdir(dir string) error {
	opts := rest.Opts{
		Method: "GET",
		Path:   "/api/v0/files/mkdir",
		Parameters: url.Values{
			"arg": []string{dir},
		},
	}
	_, err := a.srv.Call(&opts)
	if err != nil {
		return err
	}
	return nil
}

// /api/v0/files/rm
func (a *Api) FilesRm(dir string) error {
	opts := rest.Opts{
		Method: "GET",
		Path:   "/api/v0/files/rm",
		Parameters: url.Values{
			"arg":       []string{dir},
			"recursive": []string{"true"},
		},
	}
	_, err := a.srv.Call(&opts)
	if err != nil {
		return err
	}
	return nil
}

// /api/v0/object/links
func (a *Api) ObjectLinks(objectPath string) (result *Links, err error) {
	opts := rest.Opts{
		Method: "GET",
		Path:   "/api/v0/object/links",
		Parameters: url.Values{
			"arg": []string{objectPath},
		},
	}
	_, err = a.srv.CallJSON(&opts, nil, &result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// /api/v0/object/stat
func (a *Api) ObjectStat(objectPath string) (result *Stat, err error) {
	opts := rest.Opts{
		Method: "GET",
		Path:   "/api/v0/object/stat",
		Parameters: url.Values{
			"arg": []string{objectPath},
		},
	}
	_, err = a.srv.CallJSON(&opts, nil, &result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// /api/v0/object/patch/add-lin
func (a *Api) ObjectPatchAddLink(rootHash string, path string, linkHash string) (result *HasHash, err error) {
	opts := rest.Opts{
		Method: "GET",
		Path:   "/api/v0/object/patch/add-link",
		Parameters: url.Values{
			"arg": []string{
				rootHash, path, linkHash,
			},
			"create": []string{"true"},
		},
	}
	_, err = a.srv.CallJSON(&opts, nil, &result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// /api/v0/object/patch/rm-link
func (a *Api) ObjectPatchRmLink(rootHash string, path string) (result *HasHash, err error) {
	opts := rest.Opts{
		Method: "GET",
		Path:   "/api/v0/object/patch/rm-link",
		Parameters: url.Values{
			"arg": []string{
				rootHash, path,
			},
		},
	}
	_, err = a.srv.CallJSON(&opts, nil, &result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// /api/v0/object/new
func (a *Api) ObjectNewDir() (result *HasHash, err error) {
	opts := rest.Opts{
		Method: "GET",
		Path:   "/api/v0/object/new",
		Parameters: url.Values{
			"arg": []string{"unixfs-dir"},
		},
	}
	_, err = a.srv.CallJSON(&opts, nil, &result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// /api/v0/object/data
func (a *Api) ObjectData(objectPath string, options ...fs.OpenOption) (result io.ReadCloser, err error) {
	opts := rest.Opts{
		Method: "GET",
		Path:   "/api/v0/object/data",
		Parameters: url.Values{
			"arg": []string{objectPath},
		},
		Options: options,
	}
	resp, err := a.srv.Call(&opts)
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

// /api/v0/object/diff
func (a *Api) ObjectDiff(object1 string, object2 string) (result *Diff, err error) {
	opts := rest.Opts{
		Method: "GET",
		Path:   "/api/v0/object/diff",
		Parameters: url.Values{
			"arg": []string{object1, object2},
		},
	}
	_, err = a.srv.CallJSON(&opts, nil, &result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// /api/v0/ls
func (a *Api) Ls(path string) (result *Objects, err error) {
	opts := rest.Opts{
		Method: "GET",
		Path:   "/api/v0/ls",
		Parameters: url.Values{
			"arg": []string{path},
		},
	}
	_, err = a.srv.CallJSON(&opts, nil, &result)
	if err != nil {
		return nil, err
	}
	return result, nil
}
