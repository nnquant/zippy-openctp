#![allow(clippy::useless_conversion)]

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple};
use zippy_openctp_core::schema::{TickSchemaType, TICK_SCHEMA_FIELDS};
use zippy_openctp_core::{OpenCtpMarketDataSource as CoreOpenCtpMarketDataSource, OpenCtpMarketDataSourceConfig};

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

#[pyclass]
struct OpenCtpMarketDataSource {
    config: OpenCtpMarketDataSourceConfig,
    source: CoreOpenCtpMarketDataSource,
}

#[pymethods]
impl OpenCtpMarketDataSource {
    #[new]
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        front,
        broker_id,
        user_id,
        password,
        instruments,
        flow_path=".cache/openctp/md".to_string(),
        rows_per_batch=1,
        flush_interval_ms=0,
        reconnect=true,
        login_timeout_sec=10,
    ))]
    fn new(
        front: String,
        broker_id: String,
        user_id: String,
        password: String,
        instruments: Vec<String>,
        flow_path: String,
        rows_per_batch: usize,
        flush_interval_ms: u64,
        reconnect: bool,
        login_timeout_sec: u64,
    ) -> Self {
        let mut config = OpenCtpMarketDataSourceConfig::low_latency(
            front,
            broker_id,
            user_id,
            password,
            instruments,
            flow_path,
        );
        config.rows_per_batch = rows_per_batch;
        config.flush_interval_ms = flush_interval_ms;
        config.reconnect = reconnect;
        config.login_timeout_sec = login_timeout_sec;

        Self {
            source: CoreOpenCtpMarketDataSource::new(config.clone()),
            config,
        }
    }

    fn status(&self) -> &str {
        self.source.status().as_str()
    }

    fn config(&self, py: Python<'_>) -> PyResult<PyObject> {
        let dict = PyDict::new_bound(py);
        dict.set_item("front", &self.config.front)?;
        dict.set_item("broker_id", &self.config.broker_id)?;
        dict.set_item("user_id", &self.config.user_id)?;
        dict.set_item("password", "***redacted***")?;
        dict.set_item("instruments", &self.config.instruments)?;
        dict.set_item("flow_path", &self.config.flow_path)?;
        dict.set_item("reconnect", self.config.reconnect)?;
        dict.set_item("login_timeout_sec", self.config.login_timeout_sec)?;
        dict.set_item("rows_per_batch", self.config.rows_per_batch)?;
        dict.set_item("flush_interval_ms", self.config.flush_interval_ms)?;
        Ok(dict.into_py(py))
    }

    fn metrics(&self, py: Python<'_>) -> PyResult<PyObject> {
        let metrics = self.source.metrics();
        let dict = PyDict::new_bound(py);
        dict.set_item("ticks_received_total", metrics.ticks_received_total)?;
        dict.set_item("ticks_emitted_total", metrics.ticks_emitted_total)?;
        dict.set_item("batches_emitted_total", metrics.batches_emitted_total)?;
        dict.set_item("reconnects_total", metrics.reconnects_total)?;
        dict.set_item("login_failures_total", metrics.login_failures_total)?;
        dict.set_item("subscribe_failures_total", metrics.subscribe_failures_total)?;
        Ok(dict.into_py(py))
    }
}

#[pymodule]
fn _internal(py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(tick_data_schema_fields, module)?)?;
    module.add_class::<OpenCtpMarketDataSource>()?;
    let _ = py;
    Ok(())
}
