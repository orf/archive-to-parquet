import io
import tarfile
import zipfile

import pytest
from archive_to_parquet import ConversionOptions, Converter, enable_tracing
import gzip
import bz2
import zstd
import lzma
import polars as pl
import hashlib

HELLO_WORLD = b"hello world"

enable_tracing("TRACE")


def new_zip(data):
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w") as zf:
        zf.writestr("data", data)
    return out.getvalue()


def new_tar(data):
    out = io.BytesIO()
    with tarfile.TarFile(fileobj=out, mode="w") as tf:
        info = tarfile.TarInfo("data")
        info.size = len(data)
        tf.addfile(info, io.BytesIO(data))
    return out.getvalue()


compression_formats = [
    ("unknown", lambda d: d),
    ("gzip", gzip.compress),
    ("zstd", zstd.compress),
    ("bzip2", bz2.compress),
    ("xz", lzma.compress),
]

archive_formats = [
    ("zip", new_zip),
    ("tar", new_tar),
    *[
        ("tar", lambda d: compress(new_tar(d))) for f, compress in compression_formats
    ]
]


@pytest.mark.parametrize("data", [
    # Archived data
    *[
        (f, archive(HELLO_WORLD), 'data') for f, archive in archive_formats
    ],
    # Compressed data
    *[
        (f, compress(HELLO_WORLD), '') for f, compress in compression_formats
    ],
    # Nested compressed data
    *[
        (f, archive(compress(HELLO_WORLD)), 'data')
        for _, compress in compression_formats
        for f, archive in archive_formats
    ],
    # Nested archive data
    *[
        (f, archive1(archive2(HELLO_WORLD)), 'data/data')
        for f, archive1 in archive_formats
        for _, archive2 in archive_formats
    ],
])
def test_compression(tmp_path, data):
    (kind, content, nested_path) = data

    file_path = tmp_path / "text"
    file_path.write_bytes(content)
    output_path = tmp_path / "output.parquet"

    converter = Converter(ConversionOptions())
    converter.add_file(file_path)
    assert converter.inputs() == [
        (kind, str(file_path), len(content)),
    ]
    converter.convert(output_path)
    df = pl.read_parquet(output_path)
    assert df.rows(named=True) == [{
        "source": str(file_path),
        "path": str(file_path / nested_path),
        "size": len(HELLO_WORLD),
        # list() is required here, as the output seems to be a list of bytes
        # instead of a bytes object. Not sure why.
        "content": list(HELLO_WORLD),
        "hash": list(hashlib.sha256(HELLO_WORLD).digest())
    }], f'Mismatch for {kind} - file {output_path}'


def test_conversion():
    x = ConversionOptions()
    x.compression = "zstd(3)"
    assert x.compression == "ZSTD(ZstdLevel(3))"

    with pytest.raises(ValueError, match='Invalid value "foobar"'):
        x.compression = "foobar"

    for include in ("all", "binary", "all"):
        x.include = include
        assert x.include == include
    with pytest.raises(ValueError, match='Invalid value "foobar"'):
        x.include = "foobar"
