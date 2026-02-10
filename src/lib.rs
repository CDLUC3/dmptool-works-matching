use pyo3::prelude::*;

mod core;

#[pyfunction]
fn parse_name(
    text: Option<&str>,
) -> (
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
) {
    let parsed = core::parse_name(text);

    (
        parsed.first_initial,
        parsed.given_name,
        parsed.middle_initials,
        parsed.middle_names,
        parsed.surname,
        parsed.full,
    )
}

#[pyfunction]
fn revert_inverted_index(text: Option<&[u8]>) -> Option<String> {
    core::revert_inverted_index(text)
}

#[pyfunction]
#[pyo3(signature = (text, null_if_equals = None))]
fn strip_markup(text: Option<&str>, null_if_equals: Option<Vec<String>>) -> Option<String> {
    core::strip_markup(text, null_if_equals.as_deref())
}

#[pymodule]
fn _internal(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;

    // Add Python functions
    m.add_function(wrap_pyfunction!(parse_name, m)?)?;
    m.add_function(wrap_pyfunction!(revert_inverted_index, m)?)?;
    m.add_function(wrap_pyfunction!(strip_markup, m)?)?;

    // Configures logging for core functions.
    // Enable with: export RUST_LOG=dmpworks_rust=debug before running
    // transformations.
    let _ = env_logger::try_init();

    Ok(())
}