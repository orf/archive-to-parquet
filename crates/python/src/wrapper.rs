use pyo3::exceptions::PyValueError;
use pyo3::prelude::PyAnyMethods;
use pyo3::types::PyString;
use pyo3::{Bound, FromPyObject, IntoPyObject, PyAny, PyErr, PyResult, Python};
use std::fmt::{Debug, Display};
use std::str::FromStr;

#[derive(Clone, Copy, derive_new::new)]
pub struct PyStringWrapper<T> {
    pub inner: T,
}

impl<T: FromStr> FromPyObject<'_> for PyStringWrapper<T>
where
    <T as FromStr>::Err: Debug + Display,
{
    fn extract_bound(ob: &Bound<'_, PyAny>) -> PyResult<Self> {
        let s = ob.extract::<String>()?;
        Ok(Self::new(T::from_str(&s).map_err(|e| {
            PyErr::new::<PyValueError, _>(format!("Invalid value {:?} - {e}", s))
        })?))
    }
}

impl<'py, T: Display> IntoPyObject<'py> for PyStringWrapper<T> {
    type Target = PyString;
    type Output = Bound<'py, Self::Target>;
    type Error = std::convert::Infallible;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        Ok(PyString::new(py, &format!("{}", self.inner)))
    }
}
