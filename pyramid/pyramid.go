package pyramid

// FS is pyramid abstraction of filesystem where the persistent storage-layer is the block storage.
// Files on the local disk are transient and might be cleaned up by the eviction policy.
type FS interface {
	// Store adds the file from the filepath to the FS. It uploads the file to the
	// block-storage and to the localpath.
	Store(filepath, filename string) error

	// Create creates a new file in the FS.
	// It will only be persistent after the returned file is closed.
	Create(filename string) (*File, error)

	// Open finds the referenced file and returns the file descriptor.
	// If file isn't in the local disk, it is fetched from the block storage.
	Open(filename string) (*File, error)
}
