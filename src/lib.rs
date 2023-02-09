use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyTuple};
use std::collections::HashMap;

use bytes::Bytes;
use futures::future::join_all;
use reqwest;
use tokio::runtime::{Builder, Runtime};

thread_local! {
    static RUNTIME: Runtime = Builder::new_current_thread().enable_all().build().unwrap();
    static CLIENT: reqwest::Client = reqwest::Client::new();
}

async fn get_url(
    url: &str,
    method: reqwest::Method,
    head: &HashMap<&str, String>,
    body: Option<&str>,
) -> reqwest::Result<Bytes> {
    let mut req = CLIENT.with(|cl| cl.request(method, url));
    for (key, value) in head.iter() {
        req = req.header(*key, value);
    }
    req = match body {
        Some(text) => req.body(text.to_string()),
        None => req,
    };
    let b: Bytes = req.send().await?.bytes().await?;
    Ok(b)
}

async fn get_url_or(url: &str, start: usize, end: usize) -> Bytes {
    let mut headers: HashMap<&str, String> = HashMap::new();
    if (start > 0) & (end != 0) {
        headers.insert(
            "Range".as_ref(),
            format!("bytes={}-{}", start, end + 1).to_string(),
        );
    }
    let mut e: Bytes = Bytes::new();

    // Run maybe twice to deal with "connection was closing" situation
    for _ in [1..2] {
        let out = get_url(url, reqwest::Method::GET, &headers, None).await;
        if out.is_ok() {
            return out.unwrap();
        }
        e = out.err().unwrap().to_string().into()
    }
    e
}

#[pyfunction]
fn get_ranges<'a>(
    py: Python<'a>,
    urls: Vec<&str>,
    starts: Option<Vec<usize>>,
    ends: Option<Vec<usize>>,
) -> &'a PyTuple {
    let mut result: Vec<Bytes> = Vec::with_capacity(urls.len());
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
    RUNTIME.with(|rt| rt.block_on(coroutine));
    PyTuple::new(py, result.iter().map(|r| PyBytes::new(py, &r[..])))
}

/// A Python module implemented in Rust.
#[pymodule]
fn rfsspec(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(get_ranges, m)?)?;
    Ok(())
}
