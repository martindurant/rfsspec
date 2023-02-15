use bytes::Bytes;
use futures::future::join_all;
use parking_lot::Mutex;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyTuple};
use reqwest;
use std::collections::HashMap;
use std::str::FromStr;
use tokio::io::AsyncWriteExt;
use tokio::runtime::{Builder, Runtime};

static RUNTIME: Mutex<Option<Runtime>> = Mutex::new(None);
static CLIENT: Mutex<Option<reqwest::Client>> = Mutex::new(None);

fn ensure_client() {
    if CLIENT.lock().is_none() {
        *CLIENT.lock() = Some(reqwest::Client::new());
    }
}
fn ensure_runtime() {
    if RUNTIME.lock().is_none() {
        let runtime = Builder::new_current_thread()
            .worker_threads(1)
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .unwrap();
        *RUNTIME.lock() = Some(runtime);
    }
}

async fn get_file(
    url: &str, lpath: &str, method: &reqwest::Method,
    head: &HashMap<&str, String>,
) -> reqwest::Result<()> {
    let mut req = CLIENT.lock().as_ref().unwrap().request(method.into(), url);
    for (key, value) in head.iter() {
        req = req.header(*key, value);
    }
    let mut resp = req.send().await?;
    let mut out = tokio::fs::File::create(lpath).await.unwrap();
    while let Some(chunk) = resp.chunk().await? {
        out.write(chunk.as_ref()).await.unwrap();
    }
    out.flush().await.unwrap();
    Ok(())
}

/// get(urls, lpaths, headers=None, method=None)
/// --
///
/// urls: list[str]
/// lpaths: list[str]
/// headers: dict[str, str]
/// method: str | None
#[pyfunction]
#[pyo3(text_signature = "(urls, lpaths, /, headers=None, method=None)")]
fn get<'a>(
    py: Python<'a>, urls: Vec<&str>, lpaths: Vec<&str>,
    headers: Option<HashMap<&str, String>>, method: Option<&str>,
) -> () {
    let headers: HashMap<&str, String> = headers.unwrap_or(HashMap::new());
    let method = method.unwrap_or("GET");
    let method = reqwest::Method::from_str(method).unwrap();
    let coroutine = async {
        join_all(
            urls.iter()
                .zip(lpaths)
                .map(|(u, s)| get_file(u, s, &method, &headers)),
        )
        .await
        .iter()
        .count()
    };
    py.allow_threads(|| RUNTIME.lock().as_ref().unwrap().block_on(coroutine));
}

async fn get_url(
    url: &str, method: &reqwest::Method, head: &HashMap<&str, String>,
    body: Option<&str>,
) -> reqwest::Result<Bytes> {
    let mut req = CLIENT.lock().as_ref().unwrap().request(method.into(), url);
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

async fn get_url_or(
    url: &str, start: usize, end: usize, mut headers: HashMap<&str, String>,
    method: &reqwest::Method,
) -> Bytes {
    if (start > 0) & (end != 0) {
        headers.insert(
            "Range".as_ref(),
            format!("bytes={}-{}", start, end + 1).to_string(),
        );
    }

    // Run maybe twice to deal with "connection was closing" situation
    let out = get_url(url, method, &headers, None).await;
    // This is like a single-branch match
    if let Ok(content) = out {
        return content;
    }
    let out = get_url(url, method, &headers, None).await;
    if let Ok(content) = out {
        return content;
    }
    out.err().unwrap().to_string().into()
}

/// cat_ranges(urls, starts=None, ends=None, headers=None, method=None)
/// --
///
/// urls: list[str]
/// starts: list[int] | None
/// ends: list[int] | None
/// headers: dict[str, str]
/// method: str | None
#[pyfunction]
#[pyo3(
    text_signature = "(urls, /, starts=None, ends=None, headers=None, method=None)"
)]
fn cat_ranges<'a>(
    py: Python<'a>, urls: Vec<&str>, starts: Option<Vec<usize>>,
    ends: Option<Vec<usize>>, headers: Option<HashMap<&str, String>>,
    method: Option<&str>,
) -> &'a PyTuple {
    let mut result: Vec<Bytes> = Vec::with_capacity(urls.len());
    let headers: HashMap<&str, String> = headers.unwrap_or(HashMap::new());
    let method = method.unwrap_or("GET");
    let method = reqwest::Method::from_str(method).unwrap();
    let coroutine = async {
        match (starts, ends) {
            (Some(st), Some(en)) => {
                join_all(urls.iter().zip(st).zip(en).map(|((u, s), e)| {
                    get_url_or(*u, s, e, headers.clone(), &method)
                }))
                .await
                .iter()
                .map(|s| result.push(s.clone()))
                .into_iter()
                .count()
            }

            (None, None) => join_all(
                urls.iter()
                    .map(|u| get_url_or(*u, 0, 0, headers.clone(), &method)),
            )
            .await
            .iter()
            .map(|s| result.push(s.clone()))
            .into_iter()
            .count(),

            // If you only include starts or only stops, you get no results for now
            _ => 0,
        }
    };
    py.allow_threads(|| RUNTIME.lock().as_ref().unwrap().block_on(coroutine));
    PyTuple::new(py, result.iter().map(|r| PyBytes::new(py, &r[..])))
}

/// A Python module implemented in Rust.
#[pymodule]
fn rfsspec(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(cat_ranges, m)?)?;
    m.add_function(wrap_pyfunction!(get, m)?)?;
    ensure_client();
    ensure_runtime();
    Ok(())
}
