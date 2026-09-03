package artifact

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

const Format = "artifact-tar-gzip-v1"

type PipelineError struct{ Err error }

func (e PipelineError) Error() string { return e.Err.Error() }
func (e PipelineError) Unwrap() error { return e.Err }
func AsPipelineError(err error) error {
	if err == nil {
		return nil
	}
	return PipelineError{Err: err}
}
func IsPipelineError(err error) bool { var target PipelineError; return errors.As(err, &target) }

type Limits struct {
	MaxEntries, MaxPathBytes                       int
	MaxLogicalBytes, MaxArchiveBytes, MaxFileBytes int64
}

func DefaultLimits() Limits {
	return Limits{MaxEntries: 100_000, MaxPathBytes: 4096, MaxLogicalBytes: 1 << 30, MaxArchiveBytes: 512 << 20, MaxFileBytes: 1 << 30}
}

type Metadata struct {
	Name, RootName, RootKind, ContentSHA256, BlobSHA256, Format string
	ArchiveSizeBytes, LogicalSizeBytes                          int64
	EntryCount                                                  int
	CreatedAt                                                   time.Time
}

type entry struct {
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
	Entries []entry `json:"entries"`
}

func Capture(source, name, destination string, limits Limits) (Metadata, error) {
	if err := validateLimits(limits); err != nil {
		return Metadata{}, err
	}
	info, err := os.Lstat(source)
	if err != nil {
		return Metadata{}, fmt.Errorf("inspect artifact %q: %w", name, err)
	}
	rootKind := "file"
	if info.IsDir() {
		rootKind = "directory"
	} else if !info.Mode().IsRegular() {
		return Metadata{}, fmt.Errorf("artifact %q root has unsupported file type", name)
	}
	root := filepath.Base(filepath.Clean(source))
	entries, logical, err := scan(source, root, limits)
	if err != nil {
		return Metadata{}, fmt.Errorf("capture artifact %q: %w", name, err)
	}
	content, err := digestManifest(entries)
	if err != nil {
		return Metadata{}, err
	}
	f, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return Metadata{}, err
	}
	remove := true
	defer func() {
		_ = f.Close()
		if remove {
			_ = os.Remove(destination)
		}
	}()
	h := sha256.New()
	lw := &limitWriter{w: io.MultiWriter(f, h), remaining: limits.MaxArchiveBytes}
	gz, err := gzip.NewWriterLevel(lw, gzip.BestCompression)
	if err != nil {
		return Metadata{}, err
	}
	gz.Header.ModTime = time.Unix(0, 0)
	gz.Header.OS = 255
	tw := tar.NewWriter(gz)
	for _, e := range entries {
		hdr := &tar.Header{Name: e.Path, Mode: int64(e.Mode), ModTime: time.Unix(0, 0), AccessTime: time.Unix(0, 0), ChangeTime: time.Unix(0, 0), Format: tar.FormatPAX}
		switch e.Type {
		case "directory":
			hdr.Typeflag = tar.TypeDir
		case "symlink":
			hdr.Typeflag = tar.TypeSymlink
			hdr.Linkname = e.Target
		case "file":
			hdr.Typeflag = tar.TypeReg
			hdr.Size = e.Size
		}
		if err := tw.WriteHeader(hdr); err != nil {
			return Metadata{}, err
		}
		if e.Type == "file" {
			src, err := os.Open(e.host)
			if err != nil {
				return Metadata{}, err
			}
			fh := sha256.New()
			n, copyErr := io.Copy(io.MultiWriter(tw, fh), src)
			closeErr := src.Close()
			if copyErr != nil || closeErr != nil || n != e.Size || hex.EncodeToString(fh.Sum(nil)) != e.SHA256 {
				return Metadata{}, fmt.Errorf("artifact file changed during capture: %s", e.Path)
			}
		}
	}
	if err := tw.Close(); err != nil {
		return Metadata{}, err
	}
	if err := gz.Close(); err != nil {
		return Metadata{}, err
	}
	if err := f.Sync(); err != nil {
		return Metadata{}, err
	}
	if err := f.Close(); err != nil {
		return Metadata{}, err
	}
	stat, err := os.Stat(destination)
	if err != nil {
		return Metadata{}, err
	}
	remove = false
	return Metadata{Name: name, RootName: root, RootKind: rootKind, ContentSHA256: content, BlobSHA256: hex.EncodeToString(h.Sum(nil)), Format: Format, ArchiveSizeBytes: stat.Size(), LogicalSizeBytes: logical, EntryCount: len(entries), CreatedAt: time.Now().UTC()}, nil
}

func scan(source, root string, limits Limits) ([]entry, int64, error) {
	abs, err := filepath.Abs(source)
	if err != nil {
		return nil, 0, err
	}
	var out []entry
	var logical int64
	err = filepath.WalkDir(abs, func(host string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(abs, host)
		if err != nil {
			return err
		}
		name := root
		if rel != "." {
			name = path.Join(root, filepath.ToSlash(rel))
		}
		if unsafePath(name) || len(name) > limits.MaxPathBytes {
			return fmt.Errorf("unsafe artifact path %q", name)
		}
		info, err := os.Lstat(host)
		if err != nil {
			return err
		}
		e := entry{Path: name, Mode: uint32(info.Mode().Perm()), host: host}
		switch {
		case info.IsDir():
			e.Type = "directory"
		case info.Mode().IsRegular():
			e.Type = "file"
			e.Size = info.Size()
			if e.Size > limits.MaxFileBytes {
				return fmt.Errorf("file %q exceeds size limit", name)
			}
			logical += e.Size
			if logical > limits.MaxLogicalBytes {
				return fmt.Errorf("artifact exceeds logical byte limit")
			}
			f, err := os.Open(host)
			if err != nil {
				return err
			}
			h := sha256.New()
			n, copyErr := io.Copy(h, f)
			closeErr := f.Close()
			if copyErr != nil || closeErr != nil || n != e.Size {
				return fmt.Errorf("read artifact file %q", name)
			}
			e.SHA256 = hex.EncodeToString(h.Sum(nil))
		case info.Mode()&os.ModeSymlink != 0:
			e.Type = "symlink"
			target, err := os.Readlink(host)
			if err != nil {
				return err
			}
			target = filepath.ToSlash(target)
			if unsafeLink(target) {
				return fmt.Errorf("unsafe symlink %q", name)
			}
			resolved := path.Clean(path.Join(path.Dir(name), target))
			if resolved != root && !strings.HasPrefix(resolved, root+"/") {
				return fmt.Errorf("symlink %q escapes artifact root", name)
			}
			e.Target = target
		default:
			return fmt.Errorf("unsupported artifact file type at %q", name)
		}
		out = append(out, e)
		if len(out) > limits.MaxEntries {
			return fmt.Errorf("artifact exceeds entry limit")
		}
		return nil
	})
	if err != nil {
		return nil, 0, err
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out, logical, nil
}

func digestManifest(entries []entry) (string, error) {
	clean := append([]entry(nil), entries...)
	for i := range clean {
		clean[i].host = ""
	}
	b, err := json.Marshal(manifest{Version: Format, Entries: clean})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(append(b, '\n'))
	return hex.EncodeToString(sum[:]), nil
}

func Extract(archive, destination string, expected Metadata, limits Limits) error {
	if err := validateMetadata(expected, limits); err != nil {
		return err
	}
	f, err := os.Open(archive)
	if err != nil {
		return err
	}
	defer f.Close()
	stat, err := f.Stat()
	if err != nil || stat.Size() != expected.ArchiveSizeBytes {
		return fmt.Errorf("artifact archive size mismatch")
	}
	h := sha256.New()
	n, err := io.Copy(h, io.LimitReader(f, expected.ArchiveSizeBytes+1))
	if err != nil || n != expected.ArchiveSizeBytes || hex.EncodeToString(h.Sum(nil)) != expected.BlobSHA256 {
		return fmt.Errorf("artifact blob digest mismatch")
	}
	if _, err = f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	gz, err := gzip.NewReader(f)
	if err != nil {
		return fmt.Errorf("open artifact gzip: %w", err)
	}
	tr := tar.NewReader(gz)
	type link struct{ name, target string }
	var links []link
	type directory struct {
		name string
		mode fs.FileMode
	}
	var directories []directory
	entries := 0
	var total int64
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			_ = gz.Close()
			return fmt.Errorf("read artifact archive: %w", err)
		}
		entries++
		if entries > limits.MaxEntries || entries > expected.EntryCount {
			return fmt.Errorf("artifact exceeds entry limit")
		}
		if unsafePath(hdr.Name) || len(hdr.Name) > limits.MaxPathBytes {
			return fmt.Errorf("unsafe artifact archive path %q", hdr.Name)
		}
		if hdr.Name != expected.RootName && !strings.HasPrefix(hdr.Name, expected.RootName+"/") {
			return fmt.Errorf("artifact archive has unexpected root")
		}
		target := filepath.Join(destination, filepath.FromSlash(hdr.Name))
		if !within(destination, target) {
			return fmt.Errorf("artifact archive path escapes destination")
		}
		if err := safeParents(destination, filepath.Dir(target)); err != nil {
			return err
		}
		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.Mkdir(target, 0700); err != nil && !errors.Is(err, os.ErrExist) {
				return err
			}
			directories = append(directories, directory{target, fs.FileMode(hdr.Mode) & 0777})
		case tar.TypeReg, tar.TypeRegA:
			if hdr.Size < 0 || hdr.Size > limits.MaxFileBytes {
				return fmt.Errorf("artifact file exceeds limit")
			}
			total += hdr.Size
			if total > limits.MaxLogicalBytes || total > expected.LogicalSizeBytes {
				return fmt.Errorf("artifact exceeds logical byte limit")
			}
			out, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, fs.FileMode(hdr.Mode)&0777)
			if err != nil {
				return err
			}
			copied, copyErr := io.CopyN(out, tr, hdr.Size)
			closeErr := out.Close()
			if copyErr != nil || closeErr != nil || copied != hdr.Size {
				return fmt.Errorf("extract artifact file %q", hdr.Name)
			}
			if err := os.Chmod(target, fs.FileMode(hdr.Mode)&0777); err != nil {
				return err
			}
		case tar.TypeSymlink:
			targetName := filepath.ToSlash(hdr.Linkname)
			if unsafeLink(targetName) {
				return fmt.Errorf("unsafe artifact symlink")
			}
			resolved := path.Clean(path.Join(path.Dir(hdr.Name), targetName))
			if resolved != expected.RootName && !strings.HasPrefix(resolved, expected.RootName+"/") {
				return fmt.Errorf("artifact symlink escapes root")
			}
			links = append(links, link{target, targetName})
		default:
			return fmt.Errorf("unsupported artifact archive entry type %d", hdr.Typeflag)
		}
	}
	if err := gz.Close(); err != nil {
		return err
	}
	if entries != expected.EntryCount || total != expected.LogicalSizeBytes {
		return fmt.Errorf("artifact metadata mismatch")
	}
	for _, item := range links {
		if err := safeParents(destination, filepath.Dir(item.name)); err != nil {
			return err
		}
		if err := os.Symlink(item.target, item.name); err != nil {
			return err
		}
	}
	for i := len(directories) - 1; i >= 0; i-- {
		if err := os.Chmod(directories[i].name, directories[i].mode); err != nil {
			return err
		}
	}
	rescanned, _, err := scan(filepath.Join(destination, expected.RootName), expected.RootName, limits)
	if err != nil {
		return err
	}
	digest, err := digestManifest(rescanned)
	if err != nil {
		return err
	}
	if digest != expected.ContentSHA256 {
		return fmt.Errorf("artifact content digest mismatch")
	}
	return nil
}

func CopyVerified(dst io.Writer, src io.Reader, size int64, digest string, maximum int64) error {
	if size < 0 || size > maximum || !ValidDigest(digest) {
		return fmt.Errorf("invalid artifact download metadata")
	}
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(dst, h), io.LimitReader(bufio.NewReader(src), size+1))
	if err != nil {
		return err
	}
	if n != size {
		return fmt.Errorf("artifact download size mismatch")
	}
	if hex.EncodeToString(h.Sum(nil)) != digest {
		return fmt.Errorf("artifact blob digest mismatch")
	}
	return nil
}
func ValidDigest(v string) bool {
	if len(v) != 64 {
		return false
	}
	_, err := hex.DecodeString(v)
	return err == nil
}
func validateLimits(l Limits) error {
	if l.MaxEntries < 1 || l.MaxPathBytes < 1 || l.MaxLogicalBytes < 1 || l.MaxArchiveBytes < 1 || l.MaxFileBytes < 1 {
		return fmt.Errorf("artifact limits must be greater than zero")
	}
	return nil
}
func validateMetadata(m Metadata, l Limits) error {
	if err := validateLimits(l); err != nil {
		return err
	}
	if m.Format != Format || !ValidDigest(m.ContentSHA256) || !ValidDigest(m.BlobSHA256) || unsafePath(m.RootName) || path.Base(m.RootName) != m.RootName || m.ArchiveSizeBytes < 0 || m.ArchiveSizeBytes > l.MaxArchiveBytes || m.LogicalSizeBytes < 0 || m.EntryCount < 1 {
		return fmt.Errorf("invalid artifact metadata")
	}
	return nil
}
func unsafePath(name string) bool {
	if name == "" || path.IsAbs(name) || strings.ContainsRune(name, 0) || strings.IndexFunc(name, unicode.IsControl) >= 0 {
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
func safeParents(root, dir string) error {
	if !within(root, dir) {
		return fmt.Errorf("artifact parent escapes destination")
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
			return fmt.Errorf("unsafe artifact parent")
		}
	}
	return nil
}

type limitWriter struct {
	w         io.Writer
	remaining int64
}

func (w *limitWriter) Write(p []byte) (int, error) {
	if int64(len(p)) > w.remaining {
		return 0, fmt.Errorf("artifact exceeds archive byte limit")
	}
	n, err := w.w.Write(p)
	w.remaining -= int64(n)
	return n, err
}
