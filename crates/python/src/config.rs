use crate::wrapper::PyStringWrapper;
use archive_to_parquet::{Compression, ConvertionOptions, IncludeType};
use byte_unit::Byte;
use pyo3::prelude::*;
use std::fmt::{Display, Formatter};
use std::num::NonZeroUsize;

#[pyclass(
    str,
    get_all,
    set_all,
    name = "ConversionOptions",
    module = "_archive_to_parquet"
)]
#[derive(Clone)]
pub struct PyConvertionOptions {
    pub threads: NonZeroUsize,
    pub include: PyStringWrapper<IncludeType>,
    pub unique: bool,
    pub compression: PyStringWrapper<Compression>,
    pub min_size: Option<u64>,
    pub max_size: Option<u64>,
    pub batch_count: usize,
    pub batch_size: u64,
}

impl From<ConvertionOptions> for PyConvertionOptions {
    fn from(value: ConvertionOptions) -> Self {
        Self {
            threads: value.threads,
            include: PyStringWrapper::new(value.include),
            unique: value.unique,
            compression: PyStringWrapper::new(value.compression),
            min_size: value.min_size.map(|v| v.as_u64()),
            max_size: value.max_size.map(|v| v.as_u64()),
            batch_count: value.batch_count,
            batch_size: value.batch_size.into(),
        }
    }
}

impl From<PyConvertionOptions> for ConvertionOptions {
    fn from(val: PyConvertionOptions) -> Self {
        (&val).into()
    }
}

impl From<&PyConvertionOptions> for ConvertionOptions {
    fn from(val: &PyConvertionOptions) -> Self {
        ConvertionOptions {
            threads: val.threads,
            include: val.include.inner,
            unique: val.unique,
            compression: val.compression.inner,
            min_size: val.min_size.map(Byte::from),
            max_size: val.max_size.map(Byte::from),
            batch_count: val.batch_count,
            batch_size: val.batch_size.into(),
        }
    }
}

impl Display for PyConvertionOptions {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let opts: ConvertionOptions = self.into();
        write!(f, "{}", opts)
    }
}

#[pymethods]
impl PyConvertionOptions {
    #[new]
    fn py_new() -> Self {
        ConvertionOptions::const_default().into()
    }
}
