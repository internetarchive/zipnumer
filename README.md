# zipnumer

A fast CLI tool to recompress a sorted CDX stream into a [ZipNum](https://web.archive.org/web/20160804001009/http://aaron.blog.archive.org/2013/05/28/zipnum-and-cdx-cluster-merging/)-style blocked CDX file, along with its secondary index.

The output is a series of concatenated gzip members, each holding a fixed number of CDX lines, and a `.cdx.idx` file with one line per block:

```
<key> <TAB> <filename> <TAB> <offset> <TAB> <size>
```

where `<key>` is the first two space-delimited fields (SURT and timestamp) of the first CDX line in the block.

## Features

- Reads plain or gzip-compressed input (auto-detected by magic bytes)
- Reads from a file path or STDIN (`-`)
- Deterministic gzip output (no file name, zero mtime, similar to `gzip -n`)
- Detects a `CDX` header line at the start of the stream and takes the first block's key from the first non-header line
- Concurrent compression with a worker pool and a single ordered writer that computes offsets and sizes

## Installation

Requires [Go](https://go.dev/dl/) 1.21 or later.

```
go install github.com/internetarchive/zipnumer@latest
```

This places the `zipnumer` binary in `$(go env GOPATH)/bin`, which should be in your `PATH`.

Alternatively, build from source:

```
git clone https://github.com/internetarchive/zipnumer.git
cd zipnumer
go build
```

## Usage

```
$ zipnumer -h
Usage: zipnumer <input.cdx[.gz] | -> -o <output.cdx.gz> [options]

Options:
  -idx string
    	Index output path (default: IA-style .cdx.idx next to output).
  -idx-prefix string
    	Optional path prefix for CDX filename in idx (e.g., /prod/cdx/).
  -j int
    	Number of concurrent compression workers (default: NumCPU).
  -level int
    	gzip compression level 1-9 (default 6).
  -n int
    	Lines per gzip member (block). Default 6000.
  -o string
    	Output .cdx.gz path (required).
```

The output must be a file path (not `-`), because index offsets are file-based.
Unless `-idx` is given, the index is written next to the output, replacing the `.cdx.gz` suffix with `.cdx.idx` (e.g., `output.cdx.gz` -> `output.cdx.idx`).

### Examples

Recompress a CDX file into blocks of 6000 lines using 8 workers:

```
zipnumer input.cdx.gz -o output.cdx.gz -n 6000 -j 8
```

Read from STDIN with larger blocks:

```
zcat big.cdx.gz | zipnumer - -o big.blocked.cdx.gz -n 50000 -j 16
```

Prefix the filename recorded in the index:

```
zipnumer input.cdx -o part-00001.cdx.gz -idx-prefix /prod/cdx/
```

## License

[AGPL-3.0](LICENSE)
