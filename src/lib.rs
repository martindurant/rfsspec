use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyTuple};
use std::collections::HashMap;

use bytes::Bytes;
use futures::future::join_all;
use lazy_static::lazy_static;
use reqwest;
use tokio::runtime::{Builder, Runtime};

lazy_static! {
    static ref RUNTIME: Runtime = Builder::new_current_thread().enable_all().build().unwrap();
}
lazy_static! {
    static ref CLIENT: reqwest::Client = reqwest::Client::new();
}

async fn get_url(
    url: &str,
    method: reqwest::Method,
    head: HashMap<&str, String>,
    body: Option<&str>,
) -> reqwest::Result<Vec<u8>> {
    let mut req = CLIENT.request(method, url);
    for (key, value) in head.iter() {
        req = req.header(*key, value);
    }
    req = match body {
        Some(text) => req.body(text.to_string()),
        None => req,
    };
    let b: Bytes = req.send().await?.bytes().await?;
    let u: Vec<u8> = Vec::from(b);
    Ok(u)
}

async fn get_url_or(url: &str, start: usize, end: usize) -> Vec<u8> {
    let mut headers: HashMap<&str, String> = HashMap::new();
    if (start > 0) & (end != 0) {
        headers.insert(
            "Range".as_ref(),
            format!("bytes={}-{}", start, end + 1).to_string(),
        );
    }
    match get_url(url, reqwest::Method::GET, headers, None).await {
        Ok(text) => text,
        Err(e) => e.to_string().as_bytes().into(),
    }
}

#[pyfunction]
fn get_ranges<'a>(
    py: pyo3::Python<'a>,
    urls: Vec<&str>,
    starts: Option<Vec<usize>>,
    ends: Option<Vec<usize>>,
) -> &'a PyTuple {
    let mut result: Vec<Vec<u8>> = Vec::with_capacity(urls.len());
    let coroutine = async {
        match (starts, ends) {
            (Some(st), Some(en)) => join_all(
                urls.iter()
                    .zip(st)
                    .zip(en)
                    .map(|((u, s), e)| get_url_or(*u, s, e)),
            )
            .await
            .iter()
            .map(|s| result.push(s.clone()))
            .into_iter()
            .count(),

            (None, None) => join_all(urls.iter().map(|u| get_url_or(*u, 0, 0)))
                .await
                .iter()
                .map(|s| result.push(s.clone()))
                .into_iter()
                .count(),

            // If you only include starts or only stops, you get no results for now
            _ => 0,
        }
    };
    RUNTIME.block_on(coroutine);
    PyTuple::new(py, result.iter().map(|r| PyBytes::new(py, &r[..])))
}

/// A Python module implemented in Rust.
#[pymodule]
fn rfsspec(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(get_ranges, m)?)?;
    Ok(())
}
