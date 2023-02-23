use bytes::Bytes;
use futures::future::join_all;
#[macro_use]
extern crate lazy_static;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyTuple};
use reqwest;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Mutex;
use tokio::io::AsyncWriteExt;
use tokio::runtime::{Builder, Runtime};

lazy_static! {
    static ref RUNTIME: Runtime = Builder::new_current_thread()
        .max_blocking_threads(1)
        .enable_all()
        .build()
        .unwrap();
    static ref CLIENT: reqwest::Client = reqwest::Client::new();
    static ref S3_CACHE: Mutex<HashMap<String, Client>> =
        Mutex::new(HashMap::new());
}

async fn get_file(
    url: &str, lpath: &str, method: &reqwest::Method,
    head: &HashMap<&str, String>,
) -> reqwest::Result<()> {
    let mut req = CLIENT.request(method.into(), url);
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
    py.allow_threads(|| RUNTIME.block_on(coroutine));
}

async fn get_url(
    url: &str, method: &reqwest::Method, head: &HashMap<&str, String>,
    body: Option<&str>,
) -> reqwest::Result<Bytes> {
    let mut req = CLIENT.request(method.into(), url);
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
            format!("bytes={}-{}", start, end - 1).to_string(),
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
    py.allow_threads(|| RUNTIME.block_on(coroutine));
    PyTuple::new(py, result.iter().map(|r| PyBytes::new(py, &r[..])))
}

use aws_config::profile::ProfileFileCredentialsProvider;
use aws_sdk_s3::{Client, Region};
use aws_smithy_http::result::SdkError;

async fn s3(
    region: Option<&str>, profile: Option<&str>, endpoint_url: Option<&str>,
) -> Client {
    let cname: String = vec![
        region.unwrap_or("None"),
        profile.unwrap_or("None"),
        endpoint_url.unwrap_or("None"),
    ]
    .join("-");
    if S3_CACHE.lock().unwrap().contains_key(cname.as_str()) {
        // clone is free since "client" is actually an Arc pointing to real object
        return S3_CACHE.lock().unwrap().get(cname.as_str()).unwrap().clone();
    }
    let mut shared_config = match profile {
        None => aws_config::from_env(),
        Some(pro) => aws_config::from_env().credentials_provider(
            ProfileFileCredentialsProvider::builder()
                .profile_name(pro)
                .build(),
        ),
    };
    if let Some(reg) = region {
        shared_config = shared_config.region(Region::new(String::from(reg)))
    };
    if let Some(end) = endpoint_url {
        shared_config = shared_config.endpoint_url(end)
    };
    let shared_config = shared_config.load().await;
    let client = Client::new(&shared_config);
    S3_CACHE.lock().unwrap().insert(cname, client.clone());
    client
}

#[pyfunction]
fn s3_1<'py>(
    py: Python<'py>, path: &str, region: Option<&str>, profile: Option<&str>,
    endpoint_url: Option<&str>, start: Option<usize>, end: Option<usize>,
) -> &'py PyBytes {
    let out = path.split_once("/");
    let mut result: Vec<u8> = Vec::new();
    match out {
        None => result.extend("S3 ERROR: bad path".bytes()),
        Some((bucket, key)) => {
            let coroutine = async {
                let s3_client = s3(region, profile, endpoint_url).await;
                let mut resp = s3_client.get_object().bucket(bucket).key(key);
                if let (Some(st), Some(e)) = (start, end) {
                    resp =
                        resp.set_range(Some(format!("bytes={}-{}", st, e - 1)))
                };
                let resp = resp.send().await;
                match resp {
                    // Convert the body into a string
                    //let data = object.body.collect().await.unwrap().into_bytes();
                    Ok(r) => {
                        let b = r.body.collect().await.unwrap().into_bytes();
                        result.extend(b.to_vec());
                    }
                    Err(SdkError::ResponseError(e)) => {
                        result.extend(b"S3 ERRROR: ");
                        result.extend(e.raw().http().body().bytes().unwrap())
                    }
                    Err(SdkError::ServiceError(e)) => {
                        result.extend(b"S3 ERRROR: ");
                        result.extend(e.raw().http().body().bytes().unwrap())
                    }
                    Err(e) => {
                        result.extend(format!("S3 ERRROR: {}", e).as_bytes())
                    }
                }
            };
            py.allow_threads(|| RUNTIME.block_on(coroutine))
        }
    };
    PyBytes::new(py, &result[..])
}

/// A Python module implemented in Rust.
#[pymodule]
fn rfsspec(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(cat_ranges, m)?)?;
    m.add_function(wrap_pyfunction!(get, m)?)?;
    m.add_function(wrap_pyfunction!(s3_1, m)?)?;
    Ok(())
}
