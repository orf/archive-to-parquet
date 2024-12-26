use crate::config::PyConvertionOptions;
use crate::io::IOKind;
use crate::wrapper::PyStringWrapper;
use archive_to_parquet::{
    new_record_batch_channel, Converter, FormatKind, RecordBatchChannel, StandardConverter,
};
use pyo3::{pyclass, pymethods, PyResult};
use std::fs::File;
use std::io::BufReader;
use std::ops::DerefMut;
use std::path::PathBuf;
use std::sync::Mutex;
use tracing::trace;

#[pyclass(name = "Converter", module = "_archive_to_parquet")]
pub struct PyConverter {
    state: Mutex<Option<(RecordBatchChannel, StandardConverter<BufReader<IOKind>>)>>,
}

#[pymethods]
impl PyConverter {
    #[new]
    fn py_new(options: PyConvertionOptions) -> Self {
        let channel = new_record_batch_channel(options.batch_count);
        let state = Some((channel, StandardConverter::new(options.into())));
        Self {
            state: Mutex::new(state),
        }
    }

    fn add_file(&self, path: PathBuf) -> PyResult<()> {
        let mut locked = self.state.lock().unwrap();
        let Some((channel, converter)) = locked.deref_mut() else {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "add_input() called on a completed converter. Converters can only be used once.",
            ));
        };
        let file = File::open(&path)?;
        let size = file.metadata()?.len();
        let reader = IOKind::File(file);
        trace!("Adding input: {reader:?} ({size} bytes)");
        converter.add_readers([(path, size, BufReader::new(reader))], channel)?;
        Ok(())
    }

    fn inputs(&self) -> PyResult<Vec<(PyStringWrapper<FormatKind>, PathBuf, usize)>> {
        let mut locked = self.state.lock().unwrap();
        let Some((_, converter)) = locked.deref_mut() else {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "inputs() called on a completed converter. Converters can only be used once.",
            ));
        };
        Ok(converter
            .entry_details()
            .map(|(k, d)| (PyStringWrapper::new(k), d.path.clone(), d.size as usize))
            .collect())
    }

    fn convert(&self, path: PathBuf) -> PyResult<()> {
        let mut locked = self.state.lock().unwrap();
        let Some((channel, converter)) = locked.take() else {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "convert() called on a completed converter. Converters can only be used once.",
            ));
        };
        trace!(
            "Starting conversion: {} inputs",
            converter.entry_details().count()
        );
        let file = File::create(&path)?;
        let writer = IOKind::File(file);
        if let Err(e) = converter.convert(writer, channel) {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(e.to_string()));
        }
        Ok(())
    }
}
