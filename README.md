# archive-to-parquet

This is a small tool that reads a number of archive files and writes the content to a parquet file.

Features:
- Supports zip, tar, tar.gz archives
- Archive members are hashed with SHA256, which is included in the output
- Recursive extraction of archives within archives

## Example: extracting all files within a Docker image

```shell
$ skopeo copy docker://python:latest oci:docker-image/ --all
$ archive-to-parquet output.parquet docker-image/blobs/sha256/* --depth=1
2024-11-28T22:45:52.885030Z  INFO extract: archive_to_parquet::formats: Output 5 records from docker-image/blobs/sha256/84bd722ec005c4b9a8d4ce74d1547245ee36e178a58fbca74ea8a88b83557a2a depth=0 self=tar.gz
...
2024-11-28T22:45:59.885030Z  INFO All done. Wrote 145124 rows
```

## Usage

```bash
$ archive-to-parquet --help
Usage: archive-to-parquet [OPTIONS] <OUTPUT> [PATHS]...

Arguments:
  <OUTPUT>    Output Parquet file to create
  [PATHS]...  Input paths to read

Options:
  -d, --depth <DEPTH>  [default: 0]
  -h, --help           Print help
```
