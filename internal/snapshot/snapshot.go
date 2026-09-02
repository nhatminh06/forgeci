package snapshot

import (
	"archive/tar"
	"bufio"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"
	"unicode"
)

const Format = "tar-gzip-v1"

type Limits struct {
	MaxEntries      int
	MaxLogicalBytes int64
	MaxArchiveBytes int64
	MaxPathBytes    int
	MaxFileBytes    int64
}

func DefaultLimits() Limits {
	return Limits{MaxEntries: 100_000, MaxLogicalBytes: 1 << 30, MaxArchiveBytes: 512 << 20, MaxPathBytes: 4096, MaxFileBytes: 1 << 30}
}

type Metadata struct {
	SourceDigest     string    `json:"source_digest"`
	BlobDigest       string    `json:"blob_digest"`
	Format           string    `json:"format"`
	ArchiveSizeBytes int64     `json:"archive_size_bytes"`
	LogicalSizeBytes int64     `json:"logical_size_bytes"`
	EntryCount       int       `json:"entry_count"`
	CreatedAt        time.Time `json:"created_at"`
}

type Entry struct {
	Path   string `json:"path"`
	Type   string `json:"type"`
	Mode   uint32 `json:"mode"`
	Size   int64  `json:"size,omitempty"`
	SHA256 string `json:"sha256,omitempty"`
	Target string `json:"target,omitempty"`
	host   string
}

type manifest struct {
	Version string  `json:"version"`
	Entries []Entry `json:"entries"`
}

type Store struct {
	root   string
	limits Limits
}

func Open(root, sourceRoot string, limits Limits) (*Store, error) {
	if limits.MaxEntries < 1 || limits.MaxLogicalBytes < 1 || limits.MaxArchiveBytes < 1 || limits.MaxPathBytes < 1 || limits.MaxFileBytes < 1 {
		return nil, fmt.Errorf("snapshot limits must be greater than zero")
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve snapshot directory: %w", err)
	}
	if err := os.MkdirAll(abs, 0700); err != nil {
		return nil, fmt.Errorf("create snapshot directory: %w", err)
	}
	abs, err = filepath.EvalSymlinks(abs)
	if err != nil {
		return nil, fmt.Errorf("canonicalize snapshot directory: %w", err)
	}
	source, err := filepath.EvalSymlinks(sourceRoot)
	if err != nil {
		return nil, fmt.Errorf("resolve source workspace: %w", err)
	}
	source, err = filepath.Abs(source)
	if err != nil {
		return nil, err
	}
	if within(source, abs) || within(abs, source) {
		return nil, fmt.Errorf("snapshot directory and source workspace must not contain one another")
	}
	if err := os.MkdirAll(filepath.Join(abs, "blobs", "sha256"), 0700); err != nil {
		return nil, fmt.Errorf("create snapshot directory: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(abs, "tmp"), 0700); err != nil {
		return nil, fmt.Errorf("create snapshot temporary directory: %w", err)
	}
	s := &Store{root: abs, limits: limits}
	if err := s.CleanupTemps(24 * time.Hour); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Store) Root() string   { return s.root }
func (s *Store) Limits() Limits { return s.limits }

func (s *Store) blobPath(digest string) (string, error) {
	if !validDigest(digest) {
		return "", fmt.Errorf("invalid snapshot digest")
	}
	return filepath.Join(s.root, "blobs", "sha256", digest[:2], digest[2:]), nil
}

func (s *Store) OpenBlob(digest string) (*os.File, error) {
	p, err := s.blobPath(digest)
	if err != nil {
		return nil, err
	}
	return os.Open(p)
}

func (s *Store) CleanupTemps(grace time.Duration) error {
	entries, err := os.ReadDir(filepath.Join(s.root, "tmp"))
	if err != nil {
		return err
	}
	cutoff := time.Now().Add(-grace)
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if info.Mode().IsRegular() && info.ModTime().Before(cutoff) {
			if err := os.Remove(filepath.Join(s.root, "tmp", entry.Name())); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Store) Capture(sourceRoot string) (Metadata, error) {
	entries, logical, err := scan(sourceRoot, s.limits)
	if err != nil {
		return Metadata{}, err
	}
	sourceDigest, err := digestManifest(entries)
	if err != nil {
		return Metadata{}, err
	}
	tmp, err := os.CreateTemp(filepath.Join(s.root, "tmp"), ".snapshot-*")
	if err != nil {
		return Metadata{}, fmt.Errorf("create snapshot temporary file: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	h := sha256.New()
	limited := &limitWriter{w: io.MultiWriter(tmp, h), remaining: s.limits.MaxArchiveBytes}
	gz, err := gzip.NewWriterLevel(limited, gzip.BestCompression)
	if err != nil {
		tmp.Close()
		return Metadata{}, err
	}
	gz.Header.ModTime = time.Unix(0, 0)
	gz.Header.OS = 255
	tw := tar.NewWriter(gz)
	for _, entry := range entries {
		header := &tar.Header{Name: entry.Path, Mode: int64(entry.Mode), ModTime: time.Unix(0, 0), AccessTime: time.Unix(0, 0), ChangeTime: time.Unix(0, 0), Uid: 0, Gid: 0, Uname: "", Gname: "", Format: tar.FormatPAX}
		switch entry.Type {
		case "directory":
			header.Typeflag = tar.TypeDir
		case "symlink":
			header.Typeflag = tar.TypeSymlink
			header.Linkname = entry.Target
		case "file":
			header.Typeflag = tar.TypeReg
			header.Size = entry.Size
		}
		if err := tw.WriteHeader(header); err != nil {
			tw.Close()
			gz.Close()
			tmp.Close()
			return Metadata{}, fmt.Errorf("write snapshot entry %q: %w", entry.Path, err)
		}
		if entry.Type == "file" {
			f, err := os.Open(entry.host)
			if err != nil {
				tw.Close()
				gz.Close()
				tmp.Close()
				return Metadata{}, err
			}
			fh := sha256.New()
			n, copyErr := io.Copy(io.MultiWriter(tw, fh), f)
			closeErr := f.Close()
			if copyErr != nil || closeErr != nil || n != entry.Size || hex.EncodeToString(fh.Sum(nil)) != entry.SHA256 {
				tw.Close()
				gz.Close()
				tmp.Close()
				return Metadata{}, fmt.Errorf("source file changed during snapshot: %s", entry.Path)
			}
		}
	}
	if err := tw.Close(); err != nil {
		gz.Close()
		tmp.Close()
		return Metadata{}, err
	}
	if err := gz.Close(); err != nil {
		tmp.Close()
		return Metadata{}, err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return Metadata{}, err
	}
	if err := tmp.Close(); err != nil {
		return Metadata{}, err
	}
	info, err := os.Stat(tmpName)
	if err != nil {
		return Metadata{}, err
	}
	blobDigest := hex.EncodeToString(h.Sum(nil))
	final, _ := s.blobPath(sourceDigest)
	if err := os.MkdirAll(filepath.Dir(final), 0700); err != nil {
		return Metadata{}, err
	}
	if existing, err := os.Open(final); err == nil {
		existingHash := sha256.New()
		n, readErr := io.Copy(existingHash, existing)
		existing.Close()
		if readErr != nil || n != info.Size() || hex.EncodeToString(existingHash.Sum(nil)) != blobDigest {
			return Metadata{}, fmt.Errorf("immutable snapshot collision for %s", sourceDigest)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return Metadata{}, err
	} else if err := os.Link(tmpName, final); err != nil {
		if !errors.Is(err, fs.ErrExist) {
			return Metadata{}, fmt.Errorf("publish snapshot: %w", err)
		}
		existing, openErr := os.Open(final)
		if openErr != nil {
			return Metadata{}, openErr
		}
		existingHash := sha256.New()
		n, readErr := io.Copy(existingHash, existing)
		existing.Close()
		if readErr != nil || n != info.Size() || hex.EncodeToString(existingHash.Sum(nil)) != blobDigest {
			return Metadata{}, fmt.Errorf("immutable snapshot collision for %s", sourceDigest)
		}
	}
	meta := Metadata{SourceDigest: sourceDigest, BlobDigest: blobDigest, Format: Format, ArchiveSizeBytes: info.Size(), LogicalSizeBytes: logical, EntryCount: len(entries), CreatedAt: time.Now().UTC()}
	return meta, nil
}

func scan(root string, limits Limits) ([]Entry, int64, error) {
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, 0, err
	}
	var entries []Entry
	var logical int64
	err = filepath.WalkDir(abs, func(host string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if host == abs {
			return nil
		}
		rel, err := filepath.Rel(abs, host)
		if err != nil {
			return err
		}
		name := filepath.ToSlash(rel)
		if d.Name() == ".git" && d.IsDir() {
			return filepath.SkipDir
		}
		if len(name) > limits.MaxPathBytes || unsafePath(name) {
			return fmt.Errorf("unsafe source path %q", name)
		}
		info, err := os.Lstat(host)
		if err != nil {
			return err
		}
		entry := Entry{Path: name, Mode: uint32(info.Mode().Perm()), host: host}
		switch {
		case info.Mode().IsDir():
			entry.Type = "directory"
		case info.Mode().IsRegular():
			entry.Type = "file"
			entry.Size = info.Size()
			if entry.Size > limits.MaxFileBytes {
				return fmt.Errorf("source file %q exceeds size limit", name)
			}
			logical += entry.Size
			if logical > limits.MaxLogicalBytes {
				return fmt.Errorf("source exceeds logical byte limit")
			}
			f, err := os.Open(host)
			if err != nil {
				return err
			}
			h := sha256.New()
			n, e := io.Copy(h, f)
			ce := f.Close()
			if e != nil || ce != nil || n != entry.Size {
				return fmt.Errorf("read source file %q", name)
			}
			entry.SHA256 = hex.EncodeToString(h.Sum(nil))
		case info.Mode()&os.ModeSymlink != 0:
			entry.Type = "symlink"
			target, err := os.Readlink(host)
			if err != nil {
				return err
			}
			if unsafeLink(filepath.ToSlash(target)) {
				return fmt.Errorf("unsafe symlink %q", name)
			}
			resolved := path.Clean(path.Join(path.Dir(name), filepath.ToSlash(target)))
			if resolved == ".." || strings.HasPrefix(resolved, "../") {
				return fmt.Errorf("symlink %q escapes source root", name)
			}
			entry.Target = filepath.ToSlash(target)
		default:
			return fmt.Errorf("unsupported source file type at %q", name)
		}
		entries = append(entries, entry)
		if len(entries) > limits.MaxEntries {
			return fmt.Errorf("source exceeds entry limit")
		}
		return nil
	})
	if err != nil {
		return nil, 0, err
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Path < entries[j].Path })
	return entries, logical, nil
}

func digestManifest(entries []Entry) (string, error) {
	clean := make([]Entry, len(entries))
	copy(clean, entries)
	for i := range clean {
		clean[i].host = ""
	}
	b, err := json.Marshal(manifest{Version: Format, Entries: clean})
	if err != nil {
		return "", err
	}
	h := sha256.Sum256(append(b, '\n'))
	return hex.EncodeToString(h[:]), nil
}

func Verify(root, expected string, limits Limits) error {
	entries, _, err := scan(root, limits)
	if err != nil {
		return err
	}
	digest, err := digestManifest(entries)
	if err != nil {
		return err
	}
	if digest != expected {
		return fmt.Errorf("source manifest digest mismatch")
	}
	return nil
}

func Extract(archive, destination string, expected Metadata, limits Limits) error {
	if expected.Format != Format || !validDigest(expected.SourceDigest) || !validDigest(expected.BlobDigest) || expected.ArchiveSizeBytes < 0 || expected.LogicalSizeBytes < 0 || expected.EntryCount < 0 {
		return fmt.Errorf("invalid snapshot metadata")
	}
	f, err := os.Open(archive)
	if err != nil {
		return err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || info.Size() != expected.ArchiveSizeBytes || info.Size() > limits.MaxArchiveBytes {
		return fmt.Errorf("snapshot archive size mismatch")
	}
	h := sha256.New()
	n, err := io.Copy(h, io.LimitReader(f, expected.ArchiveSizeBytes+1))
	if err != nil || n != expected.ArchiveSizeBytes || hex.EncodeToString(h.Sum(nil)) != expected.BlobDigest {
		return fmt.Errorf("snapshot blob digest mismatch")
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	gz, err := gzip.NewReader(f)
	if err != nil {
		return fmt.Errorf("open snapshot gzip: %w", err)
	}
	tr := tar.NewReader(gz)
	type pendingLink struct {
		name, target string
		mode         fs.FileMode
	}
	var links []pendingLink
	entries := 0
	var total int64
	for {
		header, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			gz.Close()
			return fmt.Errorf("read snapshot archive: %w", err)
		}
		entries++
		if entries > limits.MaxEntries || entries > expected.EntryCount {
			gz.Close()
			return fmt.Errorf("snapshot exceeds entry limit")
		}
		if unsafePath(header.Name) || len(header.Name) > limits.MaxPathBytes {
			gz.Close()
			return fmt.Errorf("unsafe archive path %q", header.Name)
		}
		target := filepath.Join(destination, filepath.FromSlash(header.Name))
		if !within(destination, target) {
			gz.Close()
			return fmt.Errorf("archive path escapes workspace")
		}
		if err := safeParents(destination, filepath.Dir(target)); err != nil {
			gz.Close()
			return err
		}
		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, fs.FileMode(header.Mode)&0777); err != nil {
				gz.Close()
				return err
			}
			if err := os.Chmod(target, fs.FileMode(header.Mode)&0777); err != nil {
				gz.Close()
				return err
			}
		case tar.TypeReg, tar.TypeRegA:
			if header.Size < 0 || header.Size > limits.MaxFileBytes {
				gz.Close()
				return fmt.Errorf("snapshot file exceeds limit")
			}
			total += header.Size
			if total > limits.MaxLogicalBytes || total > expected.LogicalSizeBytes {
				gz.Close()
				return fmt.Errorf("snapshot exceeds extracted byte limit")
			}
			file, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, fs.FileMode(header.Mode)&0777)
			if err != nil {
				gz.Close()
				return err
			}
			n, copyErr := io.CopyN(file, tr, header.Size)
			closeErr := file.Close()
			if copyErr != nil || closeErr != nil || n != header.Size {
				gz.Close()
				return fmt.Errorf("extract file %q", header.Name)
			}
			if err := os.Chmod(target, fs.FileMode(header.Mode)&0777); err != nil {
				gz.Close()
				return err
			}
		case tar.TypeSymlink:
			if unsafeLink(filepath.ToSlash(header.Linkname)) {
				gz.Close()
				return fmt.Errorf("unsafe archive symlink")
			}
			resolved := path.Clean(path.Join(path.Dir(header.Name), filepath.ToSlash(header.Linkname)))
			if resolved == ".." || strings.HasPrefix(resolved, "../") {
				gz.Close()
				return fmt.Errorf("archive symlink escapes workspace")
			}
			links = append(links, pendingLink{target, header.Linkname, fs.FileMode(header.Mode) & 0777})
		default:
			gz.Close()
			return fmt.Errorf("unsupported archive entry type %d", header.Typeflag)
		}
	}
	if err := gz.Close(); err != nil {
		return err
	}
	if entries != expected.EntryCount || total != expected.LogicalSizeBytes {
		return fmt.Errorf("snapshot metadata mismatch")
	}
	for _, link := range links {
		if err := safeParents(destination, filepath.Dir(link.name)); err != nil {
			return err
		}
		if err := os.Symlink(link.target, link.name); err != nil {
			return err
		}
	}
	return Verify(destination, expected.SourceDigest, limits)
}

func safeParents(root, dir string) error {
	if !within(root, dir) {
		return fmt.Errorf("archive parent escapes workspace")
	}
	rel, _ := filepath.Rel(root, dir)
	current := root
	for _, part := range strings.Split(rel, string(filepath.Separator)) {
		if part == "." || part == "" {
			continue
		}
		current = filepath.Join(current, part)
		info, err := os.Lstat(current)
		if errors.Is(err, os.ErrNotExist) {
			if err := os.Mkdir(current, 0700); err != nil {
				return err
			}
			continue
		}
		if err != nil {
			return err
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("unsafe archive parent")
		}
	}
	return nil
}

func unsafePath(name string) bool {
	if name == "" || strings.ContainsRune(name, 0) || strings.IndexFunc(name, unicode.IsControl) >= 0 || path.IsAbs(name) {
		return true
	}
	clean := path.Clean(name)
	return clean == "." || clean == ".." || strings.HasPrefix(clean, "../") || clean != name
}
func unsafeLink(name string) bool {
	return name == "" || path.IsAbs(name) || strings.ContainsRune(name, 0) || strings.IndexFunc(name, unicode.IsControl) >= 0 || path.Clean(name) != name
}
func within(root, candidate string) bool {
	rel, err := filepath.Rel(root, candidate)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}
func validDigest(v string) bool {
	if len(v) != 64 {
		return false
	}
	_, err := hex.DecodeString(v)
	return err == nil
}

type limitWriter struct {
	w         io.Writer
	remaining int64
}

func (w *limitWriter) Write(p []byte) (int, error) {
	if int64(len(p)) > w.remaining {
		return 0, fmt.Errorf("snapshot exceeds archive byte limit")
	}
	n, err := w.w.Write(p)
	w.remaining -= int64(n)
	return n, err
}

func CopyVerified(dst io.Writer, src io.Reader, expectedSize int64, expectedDigest string, maximum int64) error {
	if expectedSize < 0 || expectedSize > maximum || !validDigest(expectedDigest) {
		return fmt.Errorf("invalid snapshot download metadata")
	}
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(dst, h), io.LimitReader(bufio.NewReader(src), expectedSize+1))
	if err != nil {
		return err
	}
	if n != expectedSize {
		return fmt.Errorf("snapshot download size mismatch")
	}
	if hex.EncodeToString(h.Sum(nil)) != expectedDigest {
		return fmt.Errorf("snapshot blob digest mismatch")
	}
	return nil
}
