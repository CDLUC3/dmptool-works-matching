use pyo3::prelude::*;

mod core;

#[pyfunction]
#[pyo3(signature = (raw_given_name=None, raw_surname=None, raw_full=None))]
fn parse_name(
    raw_given_name: Option<&str>,
    raw_surname: Option<&str>,
    raw_full: Option<&str>,
) -> (
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
) {
    let parsed = core::parse_name(raw_given_name, raw_surname, raw_full);

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
#[pyo3(signature = (text, null_if_equals = None))]
fn revert_inverted_index(text: Option<&[u8]>, null_if_equals: Option<Vec<String>>) -> Option<String> {
    core::revert_inverted_index(text, null_if_equals.as_deref())
}

#[pyfunction]
#[pyo3(signature = (text, null_if_equals = None))]
fn strip_markup(text: Option<&str>, null_if_equals: Option<Vec<String>>) -> Option<String> {
    core::strip_markup(text, null_if_equals.as_deref())
}

#[pyfunction]
#[pyo3(signature = (text))]
fn has_meaningful_initials(text: Option<&str>) -> bool {
    text.map_or(false, core::has_meaningful_initials)
}

#[pymodule]
fn _internal(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;

    // Add Python functions
    m.add_function(wrap_pyfunction!(parse_name, m)?)?;
    m.add_function(wrap_pyfunction!(revert_inverted_index, m)?)?;
    m.add_function(wrap_pyfunction!(strip_markup, m)?)?;
    m.add_function(wrap_pyfunction!(has_meaningful_initials, m)?)?;

    // Configures logging for core functions.
    // Enable with: export RUST_LOG=dmpworks_rust=debug before running
    // transformations.
    let _ = env_logger::try_init();

    Ok(())
}