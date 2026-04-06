// zipnumer.go
//
// Recompress a (sorted) CDX stream into concatenated gzip members of N lines each,
// and emit a ZipNum-style index (.cdx.idx) with:  key <TAB> filename <TAB> offset <TAB> size
//
// Features:
// - Input can be gzip or plain (auto-detected by magic bytes)
// - Input can be a file path or "-" for STDIN
// - Output is always concatenated gzip members (mtime=0, name empty; gzip -n-like determinism)
// - Key is taken from the first *non-header* CDX line if the stream starts with a CDX header,
//   otherwise from the first line of each block
// - CDX header is only checked at the start of the stream (first non-blank line)
// - Concurrent compression using a worker pool; single ordered writer computes offsets/sizes
//
// Build:
//   go build -o zipnumer zipnumer.go
//
// Example:
//   ./zipnumer input.cdx.gz -o output.cdx.gz -n 6000 -j 8
//
//   zcat big.cdx.gz | ./zipnumer - -o big.blocked.cdx.gz -n 50000 -j 16

package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
)

var gzipMagic = []byte{0x1f, 0x8b}

type Block struct {
	Seq int
	Key string
	Buf []byte // uncompressed bytes for this block (N lines)
}

type Result struct {
	Seq int
	Key string
	GZ  []byte // compressed gzip member bytes
	Err error
}

func deriveIdxPath(out string) string {
	if strings.HasSuffix(out, ".cdx.gz") {
		// ".cdx.gz" -> ".cdx.idx"
		return strings.TrimSuffix(out, ".gz") + ".idx"
	}
	return out + ".idx"
}

func isBlankLine(b []byte) bool {
	return bytes.Equal(b, []byte{'\n'}) || bytes.Equal(b, []byte{'\r', '\n'}) || bytes.Equal(b, []byte{'\r'})
}

// Heuristic: after trimming leading whitespace, line starts with "CDX" and next char is whitespace/eol
func isCDXHeaderLine(line []byte) bool {
	s := bytes.TrimLeft(line, " \t")
	if !bytes.HasPrefix(s, []byte("CDX")) {
		return false
	}
	if len(s) == 3 {
		return true
	}
	switch s[3] {
	case ' ', '\t', '\r', '\n':
		return true
	default:
		return false
	}
}

// Extract "<col1> <col2>" from a space-delimited CDX line.
// Returns "" if malformed.
func extractKey(line []byte) string {
	// Find first space
	i := bytes.IndexByte(line, ' ')
	if i <= 0 {
		return ""
	}
	// Skip consecutive spaces
	j := i + 1
	for j < len(line) && line[j] == ' ' {
		j++
	}
	if j >= len(line) {
		return ""
	}
	// Find second space
	k := bytes.IndexByte(line[j:], ' ')
	if k < 0 {
		// maybe line ends after second col (rare); trim newline
		second := bytes.TrimRight(line[j:], "\r\n")
		first := string(line[:i])
		return first + " " + string(second)
	}
	k = j + k
	first := string(line[:i])
	second := string(line[j:k])
	return first + " " + second
}

func openInput(path string) (io.ReadCloser, error) {
	if path == "-" {
		// Wrap stdin in a no-op closer
		return io.NopCloser(os.Stdin), nil
	}
	return os.Open(path)
}

func detectAndWrapGzip(r io.Reader) (io.Reader, error) {
	br, ok := r.(*bufio.Reader)
	if !ok {
		br = bufio.NewReaderSize(r, 1<<20) // 1 MiB buffer helps for big streams
	}
	hdr, err := br.Peek(2)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	if len(hdr) == 2 && hdr[0] == gzipMagic[0] && hdr[1] == gzipMagic[1] {
		zr, err := gzip.NewReader(br)
		if err != nil {
			return nil, err
		}
		return zr, nil
	}
	return br, nil
}

func compressBlock(block Block, level int) Result {
	var out bytes.Buffer
	zw, err := gzip.NewWriterLevel(&out, level)
	if err != nil {
		return Result{Seq: block.Seq, Key: block.Key, Err: err}
	}
	// gzip -n like: no name, deterministic modtime
	zw.Name = ""
	zw.ModTime = time.Unix(0, 0)

	_, werr := zw.Write(block.Buf)
	cerr := zw.Close()
	if werr != nil {
		return Result{Seq: block.Seq, Key: block.Key, Err: werr}
	}
	if cerr != nil {
		return Result{Seq: block.Seq, Key: block.Key, Err: cerr}
	}
	// Copy bytes out (out.Bytes() references internal buffer; safe after return, but we'll copy to be explicit)
	gz := append([]byte(nil), out.Bytes()...)
	return Result{Seq: block.Seq, Key: block.Key, GZ: gz, Err: nil}
}

func readerBlocks(in io.Reader, linesPerBlock int, blockCh chan<- Block) error {
	defer close(blockCh)

	br := bufio.NewReaderSize(in, 1<<20)

	// Determine if a header exists at the very start (first non-blank line only)
	headerChecked := false
	sawHeader := false

	seq := 0
	linesInBlock := 0
	var buf bytes.Buffer
	buf.Grow(linesPerBlock * 120) // rough heuristic; avoids some reallocs

	key := ""
	keySet := false

	for {
		line, err := br.ReadBytes('\n')
		if len(line) > 0 {
			// Header check only at stream start (first non-blank line)
			if !headerChecked && !isBlankLine(line) {
				headerChecked = true
				sawHeader = isCDXHeaderLine(line)
			}

			// Establish key:
			// - If stream started with a header, only the first block may contain it.
			//   In that first block, key is from first non-header line.
			// - Otherwise, key is from first line of each block.
			if !keySet {
				if sawHeader && seq == 0 {
					if !isCDXHeaderLine(line) {
						key = extractKey(line)
						keySet = true
					}
				} else {
					key = extractKey(line)
					keySet = true
				}
			}

			buf.Write(line)
			linesInBlock++

			if linesInBlock >= linesPerBlock {
				blockCh <- Block{Seq: seq, Key: key, Buf: append([]byte(nil), buf.Bytes()...)}
				seq++
				// reset
				linesInBlock = 0
				buf.Reset()
				key = ""
				keySet = false
			}
		}

		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
	}

	// trailing partial block
	if linesInBlock > 0 {
		blockCh <- Block{Seq: seq, Key: key, Buf: append([]byte(nil), buf.Bytes()...)}
	}
	return nil
}

func main() {
	var (
		linesPerBlock = flag.Int("n", 6000, "Lines per gzip member (block). Default 6000.")
		outPath       = flag.String("o", "", "Output .cdx.gz path (required).")
		idxPath       = flag.String("idx", "", "Index output path (default: IA-style .cdx.idx next to output).")
		idxPrefix     = flag.String("idx-prefix", "", "Optional path prefix for CDX filename in idx (e.g., /prod/cdx/).")
		level         = flag.Int("level", 6, "gzip compression level 1-9 (default 6).")
		jobs          = flag.Int("j", runtime.NumCPU(), "Number of concurrent compression workers (default: NumCPU).")
	)
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s <input.cdx[.gz] | -> -o <output.cdx.gz> [options]\n\nOptions:\n", filepath.Base(os.Args[0]))
		flag.PrintDefaults()
	}
	flag.Parse()

	if flag.NArg() != 1 {
		flag.Usage()
		os.Exit(2)
	}
	inPath := flag.Arg(0)

	if *outPath == "" {
		fmt.Fprintln(os.Stderr, "Error: -o is required")
		os.Exit(2)
	}
	if *outPath == "-" {
		fmt.Fprintln(os.Stderr, "Error: output must be a file path (not '-') because offsets are file-based")
		os.Exit(2)
	}
	if *linesPerBlock <= 0 {
		fmt.Fprintln(os.Stderr, "Error: -n must be > 0")
		os.Exit(2)
	}
	if *level < 1 || *level > 9 {
		fmt.Fprintln(os.Stderr, "Error: -level must be between 1 and 9")
		os.Exit(2)
	}
	if *jobs <= 0 {
		*jobs = 1
	}

	finalIdxPath := *idxPath
	if finalIdxPath == "" {
		finalIdxPath = deriveIdxPath(*outPath)
	}
	outBase := filepath.Base(*outPath)
	var idxFilename string
	if *idxPrefix != "" {
		// Clean join to avoid double slashes
		idxFilename = filepath.Join(*idxPrefix, outBase)
	} else {
		idxFilename = outBase
	}

	// Open input
	inRC, err := openInput(inPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening input: %v\n", err)
		os.Exit(1)
	}
	defer inRC.Close()

	inReader, err := detectAndWrapGzip(inRC)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error detecting/wrapping input gzip: %v\n", err)
		os.Exit(1)
	}

	// Open outputs
	outF, err := os.Create(*outPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating output: %v\n", err)
		os.Exit(1)
	}
	defer outF.Close()

	idxF, err := os.Create(finalIdxPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating idx: %v\n", err)
		os.Exit(1)
	}
	defer idxF.Close()
	idxW := bufio.NewWriterSize(idxF, 1<<20)
	defer idxW.Flush()

	// Pipeline channels
	blockCh := make(chan Block, *jobs*2)
	resCh := make(chan Result, *jobs*2)

	// Reader goroutine
	var readErr error
	var readWG sync.WaitGroup
	readWG.Add(1)
	go func() {
		defer readWG.Done()
		readErr = readerBlocks(inReader, *linesPerBlock, blockCh)
	}()

	// Worker pool
	var wg sync.WaitGroup
	wg.Add(*jobs)
	for i := 0; i < *jobs; i++ {
		go func() {
			defer wg.Done()
			for b := range blockCh {
				resCh <- compressBlock(b, *level)
			}
		}()
	}
	go func() {
		wg.Wait()
		close(resCh)
	}()

	// Ordered writer
	pending := make(map[int]Result, *jobs*2)
	next := 0
	var offset int64 = 0

	writeResult := func(r Result) error {
		if r.Err != nil {
			return r.Err
		}
		// write gzip member bytes
		n, err := outF.Write(r.GZ)
		if err != nil {
			return err
		}
		size := int64(n)

		// idx: key \t filename \t offset \t size
		// key may be "" if malformed; that's ok.
		if _, err := fmt.Fprintf(idxW, "%s\t%s\t%d\t%d\n", r.Key, idxFilename, offset, size); err != nil {
			return err
		}
		offset += size
		return nil
	}

	var writeErr error
	for r := range resCh {
		if writeErr != nil {
			continue
		}
		pending[r.Seq] = r
		for {
			pr, ok := pending[next]
			if !ok {
				break
			}
			delete(pending, next)
			if err := writeResult(pr); err != nil {
				writeErr = err
				break
			}
			next++
		}
	}

	// Wait for reader
	readWG.Wait()

	if readErr != nil {
		fmt.Fprintf(os.Stderr, "Error reading input: %v\n", readErr)
		os.Exit(1)
	}
	if writeErr != nil {
		fmt.Fprintf(os.Stderr, "Error writing output/index: %v\n", writeErr)
		os.Exit(1)
	}

	// Ensure buffers flushed
	if err := idxW.Flush(); err != nil {
		fmt.Fprintf(os.Stderr, "Error flushing idx: %v\n", err)
		os.Exit(1)
	}
	if err := outF.Sync(); err != nil {
		// Not fatal in many pipelines, but report it
		fmt.Fprintf(os.Stderr, "Warning: output sync error: %v\n", err)
	}

	_ = offset // available if you want to print totals
}
