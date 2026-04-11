#![allow(clippy::useless_conversion)]

mod native_sink;

use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use arrow::pyarrow::ToPyArrow;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyDict, PyTuple};
use zippy_openctp_core::schema::{TickSchemaType, TICK_SCHEMA_FIELDS};
use zippy_openctp_core::{
    OpenCtpMarketDataSource as CoreOpenCtpMarketDataSource, OpenCtpMarketDataSourceConfig,
    OpenCtpSourceMetrics, OpenCtpSourceStatus,
};
use zippy_core::{Source, SourceEvent, SourceHandle, SourceSink, StreamHello, ZippyError};

use crate::native_sink::NativeCapsuleSink;

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
    source: Option<CoreOpenCtpMarketDataSource>,
    metrics: Arc<Mutex<OpenCtpSourceMetrics>>,
    status: Arc<Mutex<OpenCtpSourceStatus>>,
}

#[pyclass]
struct OpenCtpRuntimeHandle {
    handle: SourceHandle,
}

#[pymethods]
impl OpenCtpRuntimeHandle {
    fn stop(&self, py: Python<'_>) -> PyResult<()> {
        py.allow_threads(|| self.handle.stop())
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))
    }

    fn join(&self, py: Python<'_>) -> PyResult<()> {
        py.allow_threads(|| self.handle.join())
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))
    }
}

struct PyCallbackSink {
    sink: Py<PyAny>,
}

impl SourceSink for PyCallbackSink {
    fn emit(&self, event: SourceEvent) -> zippy_core::Result<()> {
        Python::with_gil(|py| -> PyResult<()> {
            let sink = self.sink.bind(py);
            match event {
                SourceEvent::Hello(StreamHello {
                    protocol_version,
                    stream_name,
                    ..
                }) => {
                    sink.call_method1("emit_hello", (stream_name, protocol_version))?;
                }
                SourceEvent::Data(batch) => {
                    let py_batch = batch
                        .to_pyarrow(py)
                        .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
                    sink.call_method1("emit_data", (py_batch,))?;
                }
                SourceEvent::Flush => {
                    sink.call_method0("emit_flush")?;
                }
                SourceEvent::Stop => {
                    sink.call_method0("emit_stop")?;
                }
                SourceEvent::Error(reason) => {
                    sink.call_method1("emit_error", (reason,))?;
                }
            }
            Ok(())
        })
        .map_err(map_py_bridge_error)
    }
}

fn map_py_bridge_error(error: PyErr) -> ZippyError {
    ZippyError::Io {
        reason: format!("python source bridge failed error=[{}]", error),
    }
}

fn metrics_to_pydict(py: Python<'_>, metrics: &OpenCtpSourceMetrics) -> PyResult<PyObject> {
    let dict = PyDict::new_bound(py);
    dict.set_item("ticks_received_total", metrics.ticks_received_total)?;
    dict.set_item("ticks_emitted_total", metrics.ticks_emitted_total)?;
    dict.set_item("batches_emitted_total", metrics.batches_emitted_total)?;
    dict.set_item("reconnects_total", metrics.reconnects_total)?;
    dict.set_item("login_failures_total", metrics.login_failures_total)?;
    dict.set_item("subscribe_failures_total", metrics.subscribe_failures_total)?;
    Ok(dict.into_py(py))
}

fn blocking_join_handle() -> (OpenCtpRuntimeHandle, std::sync::mpsc::Sender<()>) {
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let join_handle = thread::spawn(move || {
        let _ = release_rx.recv();
        Ok(())
    });
    (
        OpenCtpRuntimeHandle {
            handle: SourceHandle::new(join_handle),
        },
        release_tx,
    )
}

fn blocking_stop_handle() -> (OpenCtpRuntimeHandle, std::sync::mpsc::Sender<()>) {
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let join_handle = thread::spawn(move || Ok(()));
    let stop_fn = Box::new(move || {
        let _ = release_rx.recv();
        Ok(())
    });
    (
        OpenCtpRuntimeHandle {
            handle: SourceHandle::new_with_stop(join_handle, stop_fn),
        },
        release_tx,
    )
}

#[pyfunction]
fn runtime_handle_releases_gil(py: Python<'_>, blocking_kind: &str) -> PyResult<bool> {
    let (runtime_handle, release_tx) = match blocking_kind {
        "join" => blocking_join_handle(),
        "stop" => blocking_stop_handle(),
        _ => {
            return Err(PyRuntimeError::new_err(
                "blocking_kind must be 'join' or 'stop'",
            ))
        }
    };

    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (probe_tx, probe_rx) = std::sync::mpsc::channel();

    let blocking_kind_owned = blocking_kind.to_string();
    let blocking_thread = thread::spawn(move || {
        Python::with_gil(|py| {
            entered_tx.send(()).unwrap();
            match blocking_kind_owned.as_str() {
                "join" => runtime_handle.join(py).unwrap(),
                "stop" => runtime_handle.stop(py).unwrap(),
                _ => unreachable!(),
            }
        });
    });

    let probe_thread = thread::spawn(move || {
        Python::with_gil(|_| {
            probe_tx.send(()).unwrap();
        });
    });

    let probe_result = py.allow_threads(move || {
        let started = entered_rx.recv_timeout(Duration::from_millis(200)).is_ok();
        if !started {
            let _ = release_tx.send(());
            let _ = blocking_thread.join();
            let _ = probe_thread.join();
            return Err(PyRuntimeError::new_err(
                "blocking runtime handle thread did not start",
            ));
        }

        let probe_result = probe_rx.recv_timeout(Duration::from_millis(200)).is_ok();
        let _ = release_tx.send(());
        let _ = blocking_thread.join();
        let _ = probe_thread.join();
        Ok(probe_result)
    })?;

    Ok(probe_result)
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
        let source = CoreOpenCtpMarketDataSource::new(config.clone());
        let metrics = source.metrics_handle();
        let status = source.status_handle();

        Self {
            source: Some(source),
            metrics,
            status,
            config,
        }
    }

    fn status(&self) -> &str {
        self.status.lock().unwrap().as_str()
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
        let metrics = self.metrics.lock().unwrap().clone();
        metrics_to_pydict(py, &metrics)
    }

    fn _zippy_output_schema(&self, py: Python<'_>) -> PyResult<PyObject> {
        zippy_openctp_core::schema::tick_data_schema()
            .to_pyarrow(py)
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))
    }

    fn _zippy_source_mode(&self) -> &str {
        "pipeline"
    }

    fn _zippy_source_name(&self) -> &str {
        "openctp-market-data-source"
    }

    fn _zippy_start(&mut self, py: Python<'_>, sink: Py<PyAny>) -> PyResult<Py<OpenCtpRuntimeHandle>> {
        let source = self
            .source
            .take()
            .ok_or_else(|| PyRuntimeError::new_err("openctp source already started"))?;
        let handle = Box::new(source)
            .start(Arc::new(PyCallbackSink { sink }))
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
        Py::new(py, OpenCtpRuntimeHandle { handle })
    }

    fn _zippy_start_native(
        &mut self,
        py: Python<'_>,
        sink_capsule: &Bound<'_, PyAny>,
    ) -> PyResult<Py<OpenCtpRuntimeHandle>> {
        let source = self
            .source
            .take()
            .ok_or_else(|| PyRuntimeError::new_err("openctp source already started"))?;
        let sink = NativeCapsuleSink::from_capsule(sink_capsule)?;
        let handle = Box::new(source)
            .start(Arc::new(sink))
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
        Py::new(py, OpenCtpRuntimeHandle { handle })
    }
}

#[pymodule]
fn _internal(py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(tick_data_schema_fields, module)?)?;
    module.add_function(wrap_pyfunction!(runtime_handle_releases_gil, module)?)?;
    module.add_class::<OpenCtpMarketDataSource>()?;
    module.add_class::<OpenCtpRuntimeHandle>()?;
    let _ = py;
    Ok(())
}
