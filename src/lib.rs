mod expressions;
use pyo3::prelude::*;
use pyo3_polars::PolarsAllocator;

#[pymodule]
fn _internal(_py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;

    // Configures logging for expression plugin functions.
    // Enable with: export RUST_LOG=dmpworks_expr_plugin=debug before running
    // transformations.
    env_logger::init();

    Ok(())
}

#[global_allocator]
static ALLOC: PolarsAllocator = PolarsAllocator::new();
