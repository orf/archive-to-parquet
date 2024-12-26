// use crate::formats::{ Counts};
// use crate::{ExtractionOptions, Extractor, InputError};
// use byte_unit::Byte;
// use bytes::Bytes;
// use parquet::basic::{Compression, ZstdLevel};
// use pyo3::create_exception;
// use pyo3::exceptions::{PyException, PyValueError};
// use pyo3::prelude::*;
// use pyo3_file::PyFileLikeObject;
// use std::fmt::{Display, Formatter};
// use std::fs::File;
// use std::io::Write;
// use std::num::NonZeroUsize;
// use std::path::{Path, PathBuf};
//
// create_exception!(archive_to_parquet, ExtractorError, PyException);
//
// #[pymethods]
// impl ExtractionOptions {
//     #[allow(clippy::too_many_arguments)]
//     #[new]
//     #[pyo3(
//         signature = (
//             *,
//             min_file_size,
//             max_depth=None,
//             unique=false,
//             only_text=false,
//             max_file_size=None,
//             threads=None,
//             ignore_unsupported=true,
//             compression=None
//         )
//     )]
//     fn py_new(
//         min_file_size: usize,
//         max_depth: Option<NonZeroUsize>,
//         unique: bool,
//         only_text: bool,
//         max_file_size: Option<usize>,
//         threads: Option<NonZeroUsize>,
//         ignore_unsupported: bool,
//         compression: Option<String>,
//     ) -> PyResult<Self> {
//         let compression = match compression {
//             None => Compression::ZSTD(ZstdLevel::default()),
//             Some(v) => v
//                 .parse()
//                 .map_err(|e| PyValueError::new_err(format!("Invalid compression value: {e}")))?,
//         };
//         Ok(Self {
//             min_file_size: Byte::from(min_file_size),
//             max_file_size: max_file_size.map(Byte::from),
//             max_depth,
//             only_text,
//             unique,
//             threads: threads.unwrap_or_else(crate::default_threads),
//             ignore_unsupported,
//             compression,
//         })
//     }
// }
//
// #[derive(Debug)]
// enum OutputKind {
//     File(File),
//     FileLike(PyFileLikeObject),
// }
//
// impl Write for OutputKind {
//     fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
//         match self {
//             OutputKind::File(f) => f.write(buf),
//             OutputKind::FileLike(f) => f.write(buf),
//         }
//     }
//
//     fn flush(&mut self) -> std::io::Result<()> {
//         match self {
//             OutputKind::File(f) => f.flush(),
//             OutputKind::FileLike(f) => f.flush(),
//         }
//     }
// }
//
// #[derive(Debug)]
// #[pyclass(str, name = "Extractor", module = "archive_to_parquet")]
// struct PyExtractor {
//     extractor: Extractor<OutputKind>,
// }
//
// impl Display for PyExtractor {
//     fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
//         self.extractor.fmt(f)
//     }
// }
//
// impl PyExtractor {
//     pub fn try_new(
//         writer: anyhow::Result<OutputWriter<OutputKind>>,
//         options: ExtractionOptions,
//     ) -> PyResult<PyExtractor> {
//         let extractor = writer
//             .and_then(|writer| Extractor::new(writer, options))
//             .map_err(|e| ExtractorError::new_err(e.to_string()))?;
//         Ok(Self { extractor })
//     }
// }
//
// #[pymethods]
// impl PyExtractor {
//     #[new]
//     fn py_new(output: PyObject, options: ExtractionOptions) -> PyResult<Self> {
//         Python::with_gil(|py| {
//             if let Ok(writer) =
//                 PyFileLikeObject::with_requirements(output.clone_ref(py), false, true, false, false)
//             {
//                 return Self::try_new(
//                     OutputWriter::from_writer(OutputKind::FileLike(writer), options),
//                     options,
//                 );
//             }
//             if let Ok(path_buf) = output.extract::<PathBuf>(py) {
//                 let file = File::create(&path_buf)?;
//                 return Self::try_new(
//                     OutputWriter::from_writer_with_path(OutputKind::File(file), options, path_buf),
//                     options,
//                 );
//             }
//             Err(PyValueError::new_err(format!(
//                 "Output is neither a writable object or a path: {output:?}"
//             )))
//         })
//     }
//
//     #[pyo3(name = "extract")]
//     fn py_extract(&mut self, callback: PyObject) -> PyResult<Counts> {
//         let counts = self
//             .extractor
//             .extract_with_callback(move |p, res| {
//                 let res = match res {
//                     Ok(c) => *c,
//                     Err(e) => {
//                         let e = ExtractorError::new_err(e.to_string());
//                         Python::with_gil(|py| callback.call1(py, (p, e)).unwrap());
//                         return;
//                     }
//                 };
//                 Python::with_gil(|py| callback.call1(py, (p, res)).unwrap());
//             })
//             .map_err(|e| ExtractorError::new_err(e.to_string()))?;
//         Ok(counts)
//     }
//
//     #[pyo3(name = "add_directory")]
//     fn py_add_directory(&mut self, path: PathBuf) -> Vec<InputError> {
//         self.extractor.add_directory(&path)
//     }
//
//     #[pyo3(name = "add_path")]
//     fn py_add_path(&mut self, path: PathBuf) -> PyResult<()> {
//         self.extractor
//             .add_path(path)
//             .map_err(|e| ExtractorError::new_err(e.to_string()))
//     }
//
//     #[pyo3(name = "add_buffer")]
//     fn py_add_buffer(&mut self, path: PathBuf, buffer: Vec<u8>) -> PyResult<()> {
//         self.extractor
//             .add_buffer(path, Bytes::from(buffer))
//             .map_err(|e| ExtractorError::new_err(e.to_string()))
//     }
//
//     #[pyo3(name = "add_reader")]
//     fn py_add_reader(
//         &mut self,
//         path: PathBuf,
//         reader: PyObject,
//         format:
//     ) -> PyResult<()> {
//         Python::with_gil(|py| {
//             if let Ok(reader) =
//                 PyFileLikeObject::with_requirements(reader.clone_ref(py), true, false, false, false)
//             {
//                 self.extractor.add_reader(path, Box::new(reader), format);
//                 return Ok(());
//             }
//             Err(PyValueError::new_err(format!(
//                 "Reader is not a readable object: {reader:?}"
//             )))
//         })?;
//         Ok(())
//     }
// }
//
// #[derive(Debug)]
// #[pyclass(str, frozen, module = "archive_to_parquet")]
// pub enum InputErrorKind {
//     IO(String),
//     Empty(),
//     UnsupportedFormat(),
// }
//
// impl Display for InputErrorKind {
//     fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
//         match self {
//             InputErrorKind::IO(e) => write!(f, "{}", e),
//             InputErrorKind::Empty() => write!(f, "Empty file"),
//             InputErrorKind::UnsupportedFormat() => write!(f, "Unsupported format"),
//         }
//     }
// }
//
// #[pymethods]
// impl InputError {
//     fn path(&self) -> &Path {
//         &self.path
//     }
//
//     fn kind(&self) -> InputErrorKind {
//         match &self.error {
//             crate::formats::FormatError::Io(e) => InputErrorKind::IO(e.to_string()),
//             crate::formats::FormatError::Empty => InputErrorKind::Empty(),
//             crate::formats::FormatError::UnsupportedFormat => InputErrorKind::UnsupportedFormat(),
//         }
//     }
//
//     fn __str__(&self) -> String {
//         format!("{}: {}", self.path.display(), self.kind())
//     }
// }
//
// /// A Python module implemented in Rust.
// #[pymodule]
// fn archive_to_parquet(m: &Bound<'_, PyModule>) -> PyResult<()> {
//     m.add_class::<ExtractionOptions>()?;
//     m.add_class::<PyExtractor>()?;
//     m.add_class::<Counts>()?;
//     m.add_class::<InputError>()?;
//     m.add_class::<InputErrorKind>()?;
//     Ok(())
// }
