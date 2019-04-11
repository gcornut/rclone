package ipfs

import (
	"github.com/ncw/rclone/backend/ipfs/api"
	"github.com/ncw/rclone/fs"
	"github.com/ncw/rclone/lib/atexit"
	"github.com/pkg/errors"
	"path"
	"strings"
	"sync"
	"time"
)

type Root struct {
	sync.RWMutex
	api               *api.Client
	opt               Options
	lastPersistedHash string
	hash              string
	ipnsPath          string
	ipnsKey           string
	isMFS             bool
	isReadOnly        bool

	// Wait group for persisting background go routine
	bgPersisting sync.WaitGroup
}

func NewRoot(f *Fs) (*Root, error) {
	var ipnsKey string
	var ipnsPath string
	var isMFS bool
	isEndpointReadOnly, err := f.api.IsReadOnly()
	if err != nil {
		return nil, err
	}

	base, hash := path.Split(f.opt.IpfsRoot)

	if base == "/ipfs/" {
		// IPFS path
		fs.Logf(f, "IPFS path '"+f.opt.IpfsRoot+"' is read only!")
	} else if base == "/ipns/" {
		// IPNS path
		ipnsPath = f.opt.IpfsRoot
		ipnsHash := hash

		if !isEndpointReadOnly {
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
					"since the endpoint does not have the private key to modify it!")
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
		if isEndpointReadOnly {
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
		api:               f.api,
		opt:               f.opt,
		lastPersistedHash: hash,
		hash:              hash,
		ipnsPath:          ipnsPath,
		ipnsKey:           ipnsKey,
		isMFS:             isMFS,
		isReadOnly:        isEndpointReadOnly || !(isMFS || ipnsKey != ""),
	}

	if !r.isReadOnly {
		// Persist Fs changes periodically
		r.periodicPersist()

		// Persist Fs changes on program exit
		atexit.Register(r.persist)
	}
	return &r, nil
}

// Persist root to MFS or IPNS and pin hash
func (r *Root) persist() {
	r.bgPersisting.Wait()
	r.RLock()
	defer r.RUnlock()
	if r.hash == r.lastPersistedHash {
		return
	}

	r.bgPersisting.Add(1)
	defer r.bgPersisting.Done()

	var err error

	// Update pinned hash
	err = updatePin(r.api, r.lastPersistedHash, r.hash)
	if err != nil {
		panic(err)
	}

	// Update persisted hash
	if r.isMFS {
		err = persistToMFS(r.api, r.lastPersistedHash, r.hash)
	} else if r.ipnsKey != "" {
		err = persistToIPNS(r.api, r.lastPersistedHash, r.hash, r.ipnsPath, r.ipnsKey)
	}
	if err != nil {
		panic(err)
	}
	r.lastPersistedHash = r.hash
}

// Persist periodically
func (r *Root) periodicPersist() {
	duration := time.Duration(r.opt.UpdatePeriod)
	time.AfterFunc(duration, func() {
		r.persist()
		go r.periodicPersist()
	})
}

func listChangedPath(changes []api.ObjectChange) (paths []string) {
	for _, change := range changes {
		paths = append(paths, change.Path)
	}
	return paths
}

// Persist modified IPFS DAG to IPFS MFS
func persistToMFS(api *api.Client, lastPersistedHash string, newHash string) error {
	stat, err := api.FilesStat("/")
	if err != nil {
		return errors.Wrap(err, "could not obtain stats for MFS root directory")
	}

	// List differences before and after rclone operations
	diff, err := api.ObjectDiff(lastPersistedHash, newHash)
	if err != nil {
		return err
	}

	// MFS has been modified outside rclone
	if stat.Hash != lastPersistedHash {
		// Detect incompatible changes (abort if any)

		externalDiff, err := api.ObjectDiff(stat.Hash, lastPersistedHash)
		if err != nil {
			return err
		}

		externalChangedPaths := listChangedPath(externalDiff.Changes)
		localChangedPaths := listChangedPath(diff.Changes)

		for _, externalChangedPath := range externalChangedPaths {
			for _, localChangedPath := range localChangedPaths {
				if strings.HasPrefix(externalChangedPath, localChangedPath) ||
					strings.HasPrefix(localChangedPath, externalChangedPath) {
					return errors.New("Error: concurrent modification of the IPFS MFS. Consistency not guaranteed.")
				}
			}
		}
	}

	// Persist changes to IPFS MFS
	for _, change := range diff.Changes {
		filePath := "/" + change.Path
		var err error
		if change.Before != nil {
			err = api.FilesRm(filePath)
		}
		if change.After != nil {
			absolutePath := "/ipfs/" + change.After["/"]
			err = api.FilesCp(absolutePath, filePath)
		}
		if err != nil {
			return errors.Wrap(err, "could not update MFS at path '"+filePath+"'")
		}
	}
	fs.LogPrint(fs.LogLevelDebug, "Updated IPFS MFS to '/ipfs/"+newHash+"'.")
	return nil
}

// Persist modified IPFS DAG to IPNS
func persistToIPNS(api *api.Client, lastPersistedHash string, newHash string, ipnsPath string, ipnsKey string) error {
	result, err := api.NameResolve(ipnsPath)
	if err != nil {
		return errors.Wrap(err, "could not resolve IPNS path '"+ipnsPath+"'")
	}
	_, ipfsHash := path.Split(result.Path)
	if lastPersistedHash != ipfsHash {
		return errors.New("concurrent modification of the IPFS IPNS. Consistency not guaranteed.")
	}

	err = api.NamePublish(newHash, ipnsKey)
	if err != nil {
		return errors.Wrap(err, "could not update IPNS path '"+ipnsPath+"'")
	}
	fs.LogPrint(fs.LogLevelDebug, "Updated IPNS '"+ipnsPath+"' to path '/ipfs/"+newHash+"'.")
	return nil
}

// Pin updated IPFS DAG (un-pin old one)
func updatePin(client *api.Client, lastPersistedHash string, newHash string) error {
	// Pin new IPFS root hash
	err := client.PinAdd(newHash, true)
	if err != nil {
		return errors.Wrap(err, "could not pin hash on IPFS endpoint. Consistency not guaranteed.")
	}

	// Un-pin old IPFS root hash
	err = client.PinRm(lastPersistedHash)
	if err != nil {
		isAlreadyNotPinned := strings.Contains(err.Error(), "not pinned")
		if !isAlreadyNotPinned {
			return errors.Wrap(err, "could not un-pin old hash '"+lastPersistedHash+"'.")
		}
		// ignore error if hash is already not pinned
	}
	fs.LogPrint(fs.LogLevelDebug, "Updated pin '"+lastPersistedHash+"' to path '"+newHash+"'.")
	return nil
}
