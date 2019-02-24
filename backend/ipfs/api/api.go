package api

import (
	"github.com/ncw/rclone/fs"
	"github.com/ncw/rclone/lib/rest"
	"io"
	"net/http"
	"net/url"
	"strconv"
)

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

// Add file to IPFS
// /api/v0/add
func (a *Api) Add(in io.Reader, name string, options ...fs.OpenOption) (result *FileAdded, err error) {
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
		Options: options,
	}
	_, err = a.srv.CallJSON(&opts, nil, &result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// List file in IPFS path
// /api/v0/ls
func (a *Api) Ls(path string) ([]Link, error) {
	opts := rest.Opts{
		Method: "GET",
		Path:   "/api/v0/ls",
		Parameters: url.Values{
			"arg": []string{path},
		},
	}
	var result List
	_, err := a.srv.CallJSON(&opts, nil, &result)
	if err != nil {
		return nil, err
	}
	// Only one path provided so we get the first object links
	return result.Objects[0].Links, nil
}

// Read file in IPFS path
// /api/v0/cat
func (a *Api) Cat(objectPath string, objectSize int64, options ...fs.OpenOption) (result io.ReadCloser, err error) {
	opts := rest.Opts{
		Method: "GET",
		Path:   "/api/v0/cat",
		Parameters: url.Values{
			"arg": []string{objectPath},
		},
		Options: options,
	}

	for _, option := range options {
		seekOption, isSeek := option.(*fs.SeekOption)
		if isSeek {
			offset := strconv.FormatInt(seekOption.Offset, 10)
			opts.Parameters.Add("offset", offset)
		}
		rangeOption, isRange := option.(*fs.RangeOption)
		if isRange {
			if rangeOption.Start < 0 {
				offset := strconv.FormatInt(objectSize-rangeOption.End, 10)
				opts.Parameters.Add("offset", offset)
			} else {
				offset := strconv.FormatInt(rangeOption.Start, 10)
				opts.Parameters.Add("offset", offset)

				if rangeOption.End > rangeOption.Start {
					length := strconv.FormatInt(rangeOption.End-rangeOption.Start+1, 10)
					opts.Parameters.Add("length", length)
				}
			}
		}
	}
	resp, err := a.srv.Call(&opts)
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

// Get IPFS DAG object stat
// /api/v0/object/stat
func (a *Api) ObjectStat(objectPath string) (result *ObjectStat, err error) {
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

// Patch a IPFS DAG object by adding (or replacing) a link.
// /api/v0/object/patch/add-link
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

// Patch a IPFS DAG object by removing a link.
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

// Create a new empty dir IPFS DAG object
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

// Diff two IPFS DAG object
// /api/v0/object/diff
func (a *Api) ObjectDiff(object1 string, object2 string) (result *ObjectDiff, err error) {
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

// Get file stat in IPFS MFS
// /api/v0/files/stat
func (a *Api) FilesStat(file string) (result *HasHash, err error) {
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

// Copy IPFS file to IPFS MFS
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

// Remove a IPFS MFS file
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
