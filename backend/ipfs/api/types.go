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

// /api/v0/files/ls
type FileList struct {
	Entries []FileEntry
}

// Types of things in files/ls and files/stat
const (
	FileEntryTypeFolder = 1
	FileEntryTypeFile   = 0
	FileStatTypeFolder  = "directory"
	FileStatTypeFile    = "file"
)

// /api/v0/files/ls
type FileEntry struct {
	Name string
	Type float64
	Size float64
}

// /api/v0/files/stat
type FileStat struct {
	Name string
	Size float64
	Type string
	Hash string
}

// /api/v0/add
type FileAdded struct {
	Name string
	Size string
	Hash string
}

// /api/v0/object/links
type Links struct {
	Hash  string
	Links []Link
}

// /api/v0/object/links
type Link struct {
	Name string
	Size int64
	Hash string
	Type int32
}

// /api/v0/object/stat
type Stat struct {
	Hash           string
	NumLinks       int
	BlockSize      int
	LinksSize      int
	DataSize       int64
	CumulativeSize int
}

// /api/v0/ls
type Objects struct {
	Objects []Links
}

type HasHash struct {
	Hash string
}

// /api/v0/object/diff
type Change struct {
	Type   int
	Path   string
	Before map[string]string
	After  map[string]string
}

// /api/v0/object/diff
type Diff struct {
	Changes []Change
}
