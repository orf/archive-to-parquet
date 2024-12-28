# archive-to-parquet

This is a small tool that recursively extracts data from many archive files and writes the content to a single parquet
file.
It supports the following compression and archive formats:

- Tar
- Zip
- Gzip
- Zstd
- Bzip2
- Xz

Features:

- Archive members are hashed with SHA256, which is included in the output
- Recursive extraction of archives within archives
- Filtering by file size or type (binary/text)
- Content-based deduplication
- Speed! :rocket:

## Example: extracting all files within a Docker image

```shell
$ skopeo copy docker://python:latest oci:docker-image/ --all
$ archive-to-parquet output.parquet docker-image/blobs/**/*
  INFO archive_to_parquet: Converting 112 files to Parquet
  INFO archive_to_parquet: Options: ConvertionOptions(include=All, unique=false, compression=SNAPPY, min_size=None, size_range=None, batch_count=14, batch_size=100.00 MB)
  ...
  INFO archive_to_parquet::channel: File written in 37 seconds. size=9.43 GB, batches=415 (0 pending), entries: in=263,862 out=263,862 bytes: in=25.23 GB out=25.23 GB
```

## Usage

```bash
$ archive-to-parquet --help
Usage: archive-to-parquet [OPTIONS] <OUTPUT> <PATHS>...

Arguments:
  <OUTPUT>    Output Parquet file to create
  <PATHS>...  Input paths to read

Options:
      --min-size <MIN_SIZE>        Min file size to output. Files below this size are skipped
      --max-size <MAX_SIZE>        Max file size to output. Files above this size are skipped
      --unique                     Only output unique files by hash
      --include <INCLUDE>          Only output text files, skipping binary files [default: all] [possible values: all, text, binary]
      --threads <THREADS>          Number of threads to use when extracting. Defaults to number of CPU cores [default: 12]
      --compression <COMPRESSION>  Compression to use [default: SNAPPY]
      --batch-count <BATCH_COUNT>  Number of batches to buffer in memory at once [default: 14]
      --batch-size <BATCH_SIZE>    Maximum size of each batch in memory [default: 100MB]
  -h, --help                       Print help
```
