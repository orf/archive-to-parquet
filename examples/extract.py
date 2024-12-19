import logging
from io import BytesIO
from pathlib import Path

logging.basicConfig(format="%(name)s [%(levelname)s] %(message)s", level=logging.DEBUG)

import maturin_import_hook
from maturin_import_hook.settings import MaturinSettings

maturin_import_hook.reset_logger()
# install the import hook with default settings.
# this call must be before any imports that you want the hook to be active for.
maturin_import_hook.install(
    settings=MaturinSettings(uv=True, release=False, verbose=1)
)

import archive_to_parquet

options = archive_to_parquet.ExtractionOptions(min_file_size=10, unique=True)
print("Options:", options)

output_path = Path("test_data/output/python.parquet")
output_path.parent.mkdir(exist_ok=True)

archive_path = Path("test_data/archives/initial.tar.gz")

output = BytesIO()

extractor = archive_to_parquet.Extractor(output, options)
print("Extractor", extractor)

extractor.add_path(archive_path)
print("Added path")

print("Extracted", extractor.extract(lambda p, e: print(p, e)))
# print(len(output.getbuffer()))
output_path.write_bytes(output.getbuffer())

# print([str(x) for x in extractor.add_directory("data/docker-image/blobs/sha256/")])
#
# print(extractor)
# # print(extractor.add_path(
# #     "data/docker-image/blobs/sha256/abab87fa45d0b95db90eb47059d7e87f5a69281fe5d76fcdb7889ec5c310f44c"))
#
# # path = Path("data/docker-image/blobs/sha256/0ded5bb0b377b736e6c5f45164fe8916ada3485441e963fe2fc0671949650049")
# # print(extractor.add_buffer(path, path.read_bytes()))
#
# print("Extracted", extractor.extract_with_callback(lambda p, e: print(p, e)))
