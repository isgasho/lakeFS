package pyramid

import (
	"fmt"
	"io"
	"os"
	"path"
	"time"

	"github.com/treeverse/lakefs/block"
)

//type storageProps struct {
//	t                  StorageType
//	localDir           string
//	blockStoragePrefix string
//}

// ImmutableTierFS is a filesystem where written files are never edited.
// All files are stored in the block storage. Local paths are treated as a
// cache layer that will be evicted according to the given eviction algorithm.
type ImmutableTierFS struct {
	adaptor      block.Adapter
	localStorage *SharedLocalStorage

	// TODO: use refs anc last-access for the eviction algorithm
	refCount   map[string]int
	lastAccess map[string]time.Time

	fsName    string
	adapterNS string

	localBaseDir string
	remotePrefix string
}

const fsBlockStoragePrefix = "_lakeFS"

//// mapping between supported storage types and their prefix
//var types = map[StorageType]string{
//	StorageTypeSSTable:      "sstables",
//	StorageTypeTreeManifest: "trees",
//}

func NewTierFS(adaptor block.Adapter, localStorage *SharedLocalStorage, fsName, adapterNS string) *ImmutableTierFS {
	fs := &ImmutableTierFS{
		adaptor:      adaptor,
		refCount:     map[string]int{},
		lastAccess:   map[string]time.Time{},
		fsName:       fsName,
		adapterNS:    adapterNS,
		remotePrefix: path.Join(fsBlockStoragePrefix, fsName),
	}
	fsDir, err := localStorage.Register(fsName, fs)
	if err != nil {
		panic(err)
	}
	fs.localBaseDir = fsDir

	return fs
}

// Store adds the local file to the FS.
func (tfs *ImmutableTierFS) Store(originalPath, filename string) error {
	f, err := os.Open(originalPath)
	if err != nil {
		return fmt.Errorf("open file: %w", err)
	}

	stat, err := f.Stat()
	if err != nil {
		return fmt.Errorf("file stat: %w", err)
	}

	if err := tfs.adaptor.Put(tfs.objPointer(filename), stat.Size(), f, block.PutOpts{}); err != nil {
		return fmt.Errorf("adapter put: %w", err)
	}

	if err := f.Close(); err != nil {
		return fmt.Errorf("closing file: %w", err)
	}

	localpath := tfs.localpath(filename)
	if err := os.Rename(originalPath, localpath); err != nil {
		return fmt.Errorf("rename file: %w", err)
	}
}

func (tfs *ImmutableTierFS) Create(filename string) (*File, error) {
	localpath := tfs.localpath(filename)
	fh, err := os.Create(localpath)
	if err != nil {
		return nil, fmt.Errorf("creating file: %w", err)
	}

	return &File{
		fh:      fh,
		access:  func() {},
		release: func() {},
		close: func(size int64) error {
			return tfs.adaptor.Put(tfs.objPointer(filename), size, fh, block.PutOpts{})
		},
	}, nil
}

// Load returns the a file descriptor to the local file.
// If the file is missing from the local disk, it will try to fetch it from the block storage.
func (tfs *ImmutableTierFS) Open(filename string) (*File, error) {
	localPath := tfs.localpath(filename)
	fh, err := os.Open(localPath)
	if err != nil {
		if os.IsNotExist(err) {
			fh, err = tfs.readFromBlockStorage(filename)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, fmt.Errorf("open file: %w", err)
		}
	}

	// TODO: refs thread-safe
	tfs.refCount[filename] = tfs.refCount[filename] + 1
	return &File{
		fh: fh,
		access: func() {
			tfs.lastAccess[filename] = time.Now()
		},
		release: func() {
			tfs.refCount[filename] = tfs.refCount[filename] - 1
		},
	}, nil
}

func (tfs *ImmutableTierFS) readFromBlockStorage(filename string) (*os.File, error) {
	reader, err := tfs.adaptor.Get(tfs.objPointer(filename), 0)
	if err != nil {
		return nil, fmt.Errorf("read from block storage: %w", err)
	}
	defer reader.Close()

	localPath := tfs.localpath(filename)
	writer, err := os.Create(localPath)
	if err != nil {
		return nil, fmt.Errorf("creating file: %w", err)
	}
	defer writer.Close()

	if _, err := io.Copy(writer, reader); err != nil {
		return nil, fmt.Errorf("copying date to file: %w", err)
	}

	fh, err := os.Open(localPath)
	if err != nil {
		return nil, fmt.Errorf("open file: %w", err)
	}
	return fh, nil
}

func (tfs *ImmutableTierFS) localpath(filename string) string {
	return path.Join(tfs.localBaseDir, filename)
}

func (tfs *ImmutableTierFS) blockStoragePath(filename string) string {
	return path.Join(tfs.remotePrefix, filename)
}

func (tfs *ImmutableTierFS) objPointer(filename string) block.ObjectPointer {
	return block.ObjectPointer{
		StorageNamespace: tfs.adapterNS,
		Identifier:       tfs.blockStoragePath(filename),
	}
}
