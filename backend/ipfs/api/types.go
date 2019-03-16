package api

// ipfs api error
type Error struct {
	error
	Message string
	Code    float64
	Type    string
}

func (e *Error) Error() string {
	return e.Message
}

var _ error = &Error{}

// Types of things in files/ls
const (
	FileEntryTypeFolder = 1
	FileEntryTypeFile   = 2
)

type HasHash struct {
	Hash string
}

// /api/v0/add
type FileAdded struct {
	HasHash
	Name string
	Size string
}

// /api/v0/ls
type List struct {
	Objects []struct {
		HasHash
		Links []Link
	}
}

// /api/v0/ls
type Link struct {
	HasHash
	Name string
	Size int64
	Type int32
}

// /api/v0/object/stat
type ObjectStat struct {
	HasHash
	NumLinks       int64
	BlockSize      int64
	LinksSize      int64
	DataSize       int64
	CumulativeSize int64
}

// /api/v0/object/diff
type ObjectChange struct {
	Type   int
	Path   string
	Before map[string]string
	After  map[string]string
}

// /api/v0/object/diff
type ObjectDiff struct {
	Changes []ObjectChange
}

// /api/v0/name/resolve
type HasPath struct {
	Path string
}

// /api/v0/key/list
type Key struct {
	Name string
	Id   string
}

// /api/v0/key/list
type KeyList struct {
	Keys []Key
}
