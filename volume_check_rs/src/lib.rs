use pyo3::prelude::*;

#[derive(Clone)]
#[pyclass]
pub struct VolumeCheck {
    #[pyo3(get, set)]
    pub status: String,
    #[pyo3(get, set)]
    pub current: i64,
    #[pyo3(get, set)]
    pub baseline: Option<f64>,
    #[pyo3(get, set)]
    pub ratio: Option<f64>,
    #[pyo3(get, set)]
    pub reason: Option<String>,
}

#[pymethods]
impl VolumeCheck {
    #[new]
    #[pyo3(signature = (status, current, baseline=None, ratio=None, reason=None))]
    fn new(
        status: String,
        current: i64,
        baseline: Option<f64>,
        ratio: Option<f64>,
        reason: Option<String>,
    ) -> Self {
        VolumeCheck {
            status,
            current,
            baseline,
            ratio,
            reason,
        }
    }

    fn as_fields(&self, py: Python) -> PyResult<Py<pyo3::types::PyDict>> {
        let dict = pyo3::types::PyDict::new_bound(py);
        dict.set_item("volume_status", &self.status)?;
        dict.set_item("volume_current", self.current)?;
        dict.set_item("volume_baseline", &self.baseline)?;
        dict.set_item("volume_ratio", &self.ratio)?;
        dict.set_item("volume_reason", &self.reason)?;
        Ok(dict.unbind())
    }
}

fn median(mut values: Vec<i64>) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    values.sort_unstable();
    let len = values.len();
    if len % 2 == 0 {
        (values[len / 2 - 1] as f64 + values[len / 2] as f64) / 2.0
    } else {
        values[len / 2] as f64
    }
}

#[pyfunction]
#[pyo3(signature = (current, baselines, min_ratio = 0.5, max_ratio = 2.0))]
pub fn check_volume(
    current: i64,
    baselines: Vec<i64>,
    min_ratio: f64,
    max_ratio: f64,
) -> PyResult<VolumeCheck> {
    if current == 0 {
        return Ok(VolumeCheck {
            status: "anomaly".to_string(),
            current: 0,
            baseline: None,
            ratio: None,
            reason: Some("zero rows landed".to_string()),
        });
    }

    if baselines.is_empty() {
        return Ok(VolumeCheck {
            status: "no_baseline".to_string(),
            current,
            baseline: None,
            ratio: None,
            reason: None,
        });
    }

    let baseline = median(baselines);
    if baseline == 0.0 {
        return Ok(VolumeCheck {
            status: "no_baseline".to_string(),
            current,
            baseline: Some(0.0),
            ratio: None,
            reason: Some("baseline median is zero".to_string()),
        });
    }

    let ratio = current as f64 / baseline;

    if ratio < min_ratio {
        let reason = format!(
            "{} rows is {:.0}% of the baseline {:.0}, below the {:.0}% floor",
            current,
            ratio * 100.0,
            baseline,
            min_ratio * 100.0
        );
        return Ok(VolumeCheck {
            status: "anomaly".to_string(),
            current,
            baseline: Some(baseline),
            ratio: Some(ratio),
            reason: Some(reason),
        });
    }

    if ratio > max_ratio {
        let reason = format!(
            "{} rows is {:.0}% of the baseline {:.0}, above the {:.0}% ceiling",
            current,
            ratio * 100.0,
            baseline,
            max_ratio * 100.0
        );
        return Ok(VolumeCheck {
            status: "anomaly".to_string(),
            current,
            baseline: Some(baseline),
            ratio: Some(ratio),
            reason: Some(reason),
        });
    }

    Ok(VolumeCheck {
        status: "ok".to_string(),
        current,
        baseline: Some(baseline),
        ratio: Some(ratio),
        reason: None,
    })
}

#[pymodule]
fn volume_check_rs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(check_volume, m)?)?;
    m.add_class::<VolumeCheck>()?;
    Ok(())
}
