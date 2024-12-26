// #[cfg(feature = "py-io")]
// use pyo3_file::PyFileLikeObject;
use std::fs::File;
use std::io::{Read, Write};

#[derive(Debug)]
pub enum IOKind {
    File(File),
    // #[cfg(feature = "py-io")]
    // FileLike(PyFileLikeObject),
}

impl Write for IOKind {
    #[inline(always)]
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Self::File(f) => f.write(buf),
            // #[cfg(feature = "py-io")]
            // Self::FileLike(f) => f.write(buf),
        }
    }

    #[inline(always)]
    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Self::File(f) => f.flush(),
            // #[cfg(feature = "py-io")]
            // Self::FileLike(f) => f.flush(),
        }
    }
}

impl Read for IOKind {
    #[inline(always)]
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Self::File(f) => f.read(buf),
            // #[cfg(feature = "py-io")]
            // Self::FileLike(f) => f.read(buf),
        }
    }
}

// #[cfg(feature = "py-io")]
// impl IOKind {
//     pub fn new_writer(writer: PyObject) -> PyResult<Self> {
//         Python::with_gil(|py| -> PyResult<Self> {
//             if let Ok(writer) =
//                 PyFileLikeObject::with_requirements(writer.clone_ref(py), false, true, false, false)
//             {
//                 return Ok(Self::FileLike(writer));
//             }
//             if let Ok(path_buf) = writer.extract::<PathBuf>(py) {
//                 let file = File::create(&path_buf)?;
//                 return Ok(Self::File(file));
//             }
//             Err(PyValueError::new_err(format!(
//                 "Output is neither a writable object or a path: {writer}"
//             )))
//         })
//     }
//
//     pub fn new_reader(reader: PyObject) -> PyResult<Self> {
//         Python::with_gil(|py| -> PyResult<Self> {
//             if let Ok(writer) =
//                 PyFileLikeObject::with_requirements(reader.clone_ref(py), true, false, false, false)
//             {
//                 return Ok(Self::FileLike(writer));
//             }
//             if let Ok(path_buf) = reader.extract::<PathBuf>(py) {
//                 let file = File::open(&path_buf)?;
//                 return Ok(Self::File(file));
//             }
//             Err(PyValueError::new_err(format!(
//                 "Output is neither a readable object or a path: {reader}"
//             )))
//         })
//     }
// }
