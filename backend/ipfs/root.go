package ipfs

import (
	"errors"
	"github.com/ncw/rclone/backend/ipfs/api"
	"github.com/ncw/rclone/fs"
	"github.com/ncw/rclone/lib/atexit"
	"path"
	"strings"
	"sync"
	"time"
)

type Root struct {
	sync.RWMutex
	api         *api.Api
	initialHash string
	hash        string
	ipnsPath    string
	ipnsKey     string
	isMFS       bool
	isReadOnly  bool
}

func NewRoot(f *Fs) (*Root, error) {
	var ipnsKey string
	var ipnsPath string
	var isMFS bool
	isGateWay := f.opt.Endpoint == PublicGateway

	base, hash := path.Split(f.opt.IpfsRoot)

	if base == "/ipfs/" {
		// IPFS path
		fs.Logf(f, "IPFS path '"+f.opt.IpfsRoot+"' is read only!")
	} else if base == "/ipns/" {
		// IPNS path
		ipnsPath = f.opt.IpfsRoot
		ipnsHash := hash

		if !isGateWay {
			keys, err := f.api.KeyList()
			if err != nil {
				return nil, err
			}
			for _, Key := range keys.Keys {
				if Key.Id == ipnsHash {
					ipnsKey = Key.Name
					break
				}
			}
			if ipnsKey == "" {
				fs.Logf(f, "IPNS path '"+ipnsPath+"' is read only "+
					"since the endpoint does not have the right key to modify it!")
			}
		} else {
			fs.Logf(f, "IPNS path '"+ipnsPath+"' is read only "+
				"since the endpoint is the read only public gateway!")
		}

		// Resolve IPNS path to get the IPFS hash behind it
		result, err := f.api.NameResolve(f.opt.IpfsRoot)
		if err != nil {
			return nil, err
		}
		_, hash = path.Split(result.Path)
	} else if f.opt.IpfsRoot == "" {
		if isGateWay {
			return nil, errors.New(
				"read only public IPFS gateway can't use MFS. " +
					"Please use a IPFS path or IPNS path as `--ipfs-root` parameter")
		}
		// IPFS MFS
		stat, err := f.api.FilesStat("/")
		if err != nil {
			return nil, err
		}
		hash = stat.Hash
		isMFS = true
	} else {
		return nil, errors.New("Invalid IPFS path '" + f.opt.IpfsRoot + "'")
	}

	r := Root{
		api:         f.api,
		initialHash: hash,
		hash:        hash,
		ipnsPath:    ipnsPath,
		ipnsKey:     ipnsKey,
		isMFS:       isMFS,
		isReadOnly:  isGateWay || !(isMFS || ipnsKey != ""),
	}

	if !r.isReadOnly {
		// Persist Fs changes

		if r.isMFS {
			// periodically for MFS only
			r.periodicPersist()
		}

		// on program exit
		atexit.Register(r.persist)
	}
	return &r, nil
}

// Persist root to MFS or IPNS
func (r *Root) persist() {
	r.Lock()
	defer r.Unlock()
	if r.hash == r.initialHash {
		return
	}
	if r.isMFS {
		r.persistToMFS()
	} else if r.ipnsKey != "" {
		r.persistToIPNS()
	}
}

// Persist periodically
func (r *Root) periodicPersist() {
	nextTime := time.Now().Add(PersistPeriod)
	time.Sleep(time.Until(nextTime))
	r.persist()
	go r.periodicPersist()
}

func listChangedPath(changes []api.ObjectChange) (paths []string) {
	for _, change := range changes {
		paths = append(paths, change.Path)
	}
	return paths
}

// Persist modified IPFS DAG to IPFS MFS
func (r *Root) persistToMFS() {
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
		// Detect incompatible changes (abort if any)

		externalDiff, err := r.api.ObjectDiff(stat.Hash, r.initialHash)
		if err != nil {
			panic(err)
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
		filePath := "/" + change.Path
		if change.Before != nil {
			err := r.api.FilesRm(filePath)
			if err != nil {
				panic(err)
			}
		}
		if change.After != nil {
			absolutePath := "/ipfs/" + change.After["/"]
			err := r.api.FilesCp(absolutePath, filePath)
			if err != nil {
				panic(err)
			}
		}
	}
	fs.LogPrint(fs.LogLevelDebug, "Updated IPFS MFS to '/ipfs/"+r.hash+"'.")

	// Update hash from MFS in case it changed
	stat, err = r.api.FilesStat("/")
	if err != nil {
		panic(err)
	}
	r.hash = stat.Hash

	r.initialHash = r.hash
}

// Persist modified IPFS DAG to IPNS
func (r *Root) persistToIPNS() {
	result, err := r.api.NameResolve(r.ipnsPath)
	if err != nil {
		panic(err)
	}
	_, ipfsHash := path.Split(result.Path)
	if r.initialHash != ipfsHash {
		panic("Error: concurrent modification of the IPFS IPNS. Consistency not guaranteed.")
	}

	err = r.api.NamePublish(r.hash, r.ipnsKey)
	if err != nil {
		panic(err)
	}
	fs.LogPrint(fs.LogLevelDebug, "Updated IPNS '"+r.ipnsPath+"' to path '/ipfs/"+r.hash+"'.")

	r.initialHash = r.hash
}
