use pyo3::prelude::*;
use pyo3::types::PyTuple;
use zippy_openctp_core::schema::{TickSchemaType, TICK_SCHEMA_FIELDS};

#[pyfunction]
fn tick_data_schema_fields(py: Python<'_>) -> PyResult<PyObject> {
    let fields: Vec<PyObject> = TICK_SCHEMA_FIELDS
        .iter()
        .map(|field| {
            let type_name = match field.data_type {
                TickSchemaType::Utf8 => "utf8",
                TickSchemaType::TimestampNsUtc => "timestamp_ns_utc",
                TickSchemaType::Float64 => "float64",
                TickSchemaType::Int64 => "int64",
            };
            PyTuple::new_bound(py, [field.name.into_py(py), type_name.into_py(py), field.nullable.into_py(py)])
                .into_py(py)
        })
        .collect::<Vec<_>>();
    Ok(fields.into_py(py))
}

#[pymodule]
fn _internal(py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(tick_data_schema_fields, module)?)?;
    let _ = py;
    Ok(())
}
