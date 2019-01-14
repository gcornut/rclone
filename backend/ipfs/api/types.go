package api

// ipfs api error
type Error struct {
	error
	Message string  `json:"Message"`
	Code    float64 `json:"Code"`
	Type    string  `json:"Type"`
}

func (e *Error) Error() string {
	return e.Message
}

var _ error = &Error{}

// /api/v0/files/ls
type FileList struct {
	Entries []FileEntry `json:"Entries"`
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
	Name string  `json:"Name"`
	Type float64 `json:"Type"`
	Size float64 `json:"Size"`
}

// /api/v0/files/stat
type FileStat struct {
	Name string  `json:"Name"`
	Size float64 `json:"Size"`
	Type string  `json:"Type"`
	Hash string  `json:"Hash"`
}

// /api/v0/add
type FileAdded struct {
	Name string `json:"Name"`
	Size string `json:"Size"`
	Hash string `json:"Hash"`
}
