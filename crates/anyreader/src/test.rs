use std::io::{Read, Write};
use std::path::PathBuf;
use tar::Header;
use zip::write::SimpleFileOptions;

pub fn read_vec(mut reader: impl Read) -> Vec<u8> {
    let mut out = Vec::new();
    reader.read_to_end(&mut out).unwrap();
    out
}

pub fn gzip_data(data: impl AsRef<[u8]>) -> Vec<u8> {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(data.as_ref()).unwrap();
    encoder.finish().unwrap()
}

pub fn zstd_data(data: impl AsRef<[u8]>) -> Vec<u8> {
    zstd::encode_all(data.as_ref(), 0).unwrap()
}

pub fn bz2_data(data: impl AsRef<[u8]>) -> Vec<u8> {
    let mut encoder = bzip2::write::BzEncoder::new(Vec::new(), Default::default());
    encoder.write_all(data.as_ref()).unwrap();
    encoder.finish().unwrap()
}

pub fn xz_data(data: impl AsRef<[u8]>) -> Vec<u8> {
    let mut encoder = liblzma::write::XzEncoder::new(Vec::new(), Default::default());
    encoder.write_all(data.as_ref()).unwrap();
    encoder.finish().unwrap()
}

pub fn tar_archive(
    files: impl IntoIterator<Item = (impl Into<PathBuf>, impl AsRef<[u8]>)>,
) -> Vec<u8> {
    let mut builder = tar::Builder::new(Vec::new());
    for (path, data) in files {
        let data = data.as_ref();
        let mut header = Header::new_gnu();
        header.set_size(data.len() as u64);
        builder.append_data(&mut header, path.into(), data).unwrap();
    }

    builder.into_inner().unwrap()
}

pub fn zip_archive(
    files: impl IntoIterator<Item = (impl Into<PathBuf>, impl AsRef<[u8]>)>,
) -> Vec<u8> {
    let options = SimpleFileOptions::default().compression_method(zip::CompressionMethod::BZIP2);
    let mut a = zip::write::ZipWriter::new(std::io::Cursor::new(vec![]));
    for (path, data) in files {
        a.start_file(path.into().to_string_lossy(), options)
            .unwrap();
        a.write_all(data.as_ref()).unwrap();
    }
    a.finish().unwrap().into_inner()
}

pub fn tar_read_entries(data: impl Read) -> Vec<Vec<u8>> {
    let d = read_vec(data);
    let mut archive = tar::Archive::new(d.as_slice());
    archive
        .entries()
        .unwrap()
        .map(|i| i.map(read_vec))
        .collect::<Result<Vec<_>, _>>()
        .unwrap_or_else(|_| panic!("Error reading tar data: {d:?}"))
}
