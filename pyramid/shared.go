package pyramid

import (
	"fmt"
	"os"
	"path"
)

type SharedLocalStorage struct {
	filesystems map[string]FS
	basepath    string
}

func NewSharedLocalStorage(baseFolderPath string) (*SharedLocalStorage, error) {
	if err := os.MkdirAll(baseFolderPath, os.ModePerm); err != nil {
		return nil, fmt.Errorf("creating base dir: %w", err)
	}

	return &SharedLocalStorage{
		filesystems: map[string]FS{},
		basepath:    baseFolderPath,
	}, nil
}

func (sd *SharedLocalStorage) Register(fsName string, fs FS) (string, error) {
	if _, ok := sd.filesystems[fsName]; ok {
		return "", fmt.Errorf("file system %s already registered", fsName)
	}

	dirPath := path.Join(sd.basepath, fsName)
	if err := os.Mkdir(dirPath, os.ModePerm); err != nil {
		return "", fmt.Errorf("creating fs dir: %w", err)
	}

	sd.filesystems[fsName] = fs
	return dirPath, nil
}
