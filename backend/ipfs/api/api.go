package api

import (
	"github.com/ncw/rclone/lib/rest"
	"io"
	"net/url"
	"path"
	"strings"
)

// TODO: Add all of this in github.com/ipfs/go-ipfs-api (and add go-ipfs-api as a dependency for rclone)

// /api/v0/files/stat
func FilesStat(srv *rest.Client, file string) (stat *FileStat, err error) {
	if !strings.HasPrefix(file, "/") {
		file = "/" + file
	}
	opts := rest.Opts{
		Method: "GET",
		Path:   "/api/v0/files/stat",
		Parameters: url.Values{
			"arg": []string{file},
		},
	}

	var result FileStat
	_, err = srv.CallJSON(&opts, nil, &result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

// /api/v0/files/ls
func FilesList(srv *rest.Client, dir string) (fileEntries *FileList, err error) {
	arg := dir
	if !strings.HasPrefix(dir, "/") {
		arg = "/" + arg
	}

	opts := rest.Opts{
		Method: "GET",
		Path:   "/api/v0/files/ls",
		Parameters: url.Values{
			"arg": []string{arg},
			"l":   []string{"true"},
			"U":   []string{"true"},
		},
	}

	var result FileList
	_, err = srv.CallJSON(&opts, nil, &result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

// /api/v0/add
func Add(srv *rest.Client, in io.Reader, name string) (*FileAdded, error) {
	opts := rest.Opts{
		Method:               "POST",
		Path:                 "/api/v0/add",
		MultipartParams:      url.Values{},
		MultipartContentName: "file",
		MultipartFileName:    name,
		Body:                 in,
	}

	var result FileAdded
	_, err := srv.CallJSON(&opts, nil, &result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

// /api/v0/files/write
func FilesWrite(srv *rest.Client, file string, in io.Reader) error {
	if !strings.HasPrefix(file, "/") {
		file = "/" + file
	}
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
	_, err := srv.CallJSON(&opts, nil, nil)
	return err
}

// /api/v0/files/cp
func FilesCp(srv *rest.Client, from string, to string) error {
	if !strings.HasPrefix(from, "/") {
		from = "/" + from
	}
	if !strings.HasPrefix(to, "/") {
		to = "/" + to
	}
	opts := rest.Opts{
		Method: "GET",
		Path:   "/api/v0/files/cp",
		Parameters: url.Values{
			"arg": []string{from, to},
		},
	}

	_, err := srv.Call(&opts)
	if err != nil {
		return err
	}
	return nil
}

// /api/v0/files/mkdir
func FilesMkdir(srv *rest.Client, dir string) error {
	arg := dir
	if !strings.HasPrefix(dir, "/") {
		arg = "/" + arg
	}

	opts := rest.Opts{
		Method: "GET",
		Path:   "/api/v0/files/mkdir",
		Parameters: url.Values{
			"arg": []string{arg},
		},
	}

	_, err := srv.Call(&opts)
	if err != nil {
		return err
	}
	return nil
}

// /api/v0/files/rm
func FilesRm(srv *rest.Client, dir string) error {
	arg := dir
	if !strings.HasPrefix(dir, "/") {
		arg = "/" + arg
	}

	opts := rest.Opts{
		Method: "GET",
		Path:   "/api/v0/files/rm",
		Parameters: url.Values{
			"arg":       []string{arg},
			"recursive": []string{"true"},
		},
	}

	_, err := srv.Call(&opts)
	if err != nil {
		return err
	}
	return nil
}
