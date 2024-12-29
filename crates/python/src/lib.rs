use pyo3::prelude::*;
use std::io::stderr;
use tracing::info;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{fmt, EnvFilter};

mod config;
mod converter;
mod io;
mod wrapper;

#[pymodule(name = "archive_to_parquet")]
fn setup_module(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<config::PyConvertionOptions>()?;
    m.add_class::<converter::PyConverter>()?;
    m.add_function(wrap_pyfunction!(enable_tracing, m)?)?;
    Ok(())
}

#[pyfunction]
fn enable_tracing(level: &str) {
    let env_filter = EnvFilter::builder()
        .with_default_directive(level.parse().unwrap())
        .from_env()
        .unwrap();
    tracing_subscriber::registry()
        .with(fmt::layer().compact().with_file(false).with_writer(stderr))
        .with(env_filter)
        .try_init()
        .unwrap();
    info!("tracing enabled at level: {}", level);
}
