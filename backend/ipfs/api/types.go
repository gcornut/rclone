package api


// ipfs api error
type Error struct {
	Message string  `json:"Message"`
	Code    float64 `json:"Code"`
	Type    string  `json:"Type"`
}

// /files/ls
type FileList struct {
	Entries []FileEntry `json:"Entries"`
}

// /files/ls
type FileEntry struct {
	Name string `json:"Name"`
}

// /files/stat
type FileStat struct {
	Name string  `json:"Name"`
	Size float64 `json:"CumulativeSize"`
	Type string  `json:"Type"`
	Hash string  `json:"Hash"`
}
