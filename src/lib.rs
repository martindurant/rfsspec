use bytes::Bytes;
use futures::future::join_all;
#[macro_use]
extern crate lazy_static;
use google_auth::TokenManager;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyTuple};
use reqwest;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Mutex;
use tokio::io::AsyncWriteExt;
use tokio::runtime::{Builder, Runtime};
use urlencoding::encode;

lazy_static! {
    static ref RUNTIME: Runtime = Builder::new_current_thread()
        .max_blocking_threads(1)
        .enable_all()
        .build()
        .unwrap();
    static ref CLIENT: reqwest::Client = reqwest::Client::new();
    static ref S3_CACHE: Mutex<HashMap<String, Client>> =
        Mutex::new(HashMap::new());
    static ref GCS_TOKEN: Mutex<HashMap<String, TokenManager>> =
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
    if (start > 0) | (end != 0) {
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
    Bytes::from(format!("HTTP ERROR: {}", out.err().unwrap().to_string()))
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
use aws_sdk_s3::model::RequestPayer;
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

fn make_unsigned<O, Retry>(
    mut operation: aws_smithy_http::operation::Operation<O, Retry>,
) -> Result<
    aws_smithy_http::operation::Operation<O, Retry>,
    std::convert::Infallible,
> {
    {
        let mut props = operation.properties_mut();
        let mut signing_config = props
            .get_mut::<aws_sig_auth::signer::OperationSigningConfig>()
            .expect("has signing_config");
        signing_config.signing_requirements =
            aws_sig_auth::signer::SigningRequirements::Disabled;
    }
    Ok(operation)
}

async fn s3_get_one_range(
    url: &str, s3: Client, start: usize, end: usize, requester_pays: bool,
    anon: bool,
) -> Vec<u8> {
    let out = url.split_once("/");
    match out {
        None => b"S3 ERROR: bad path".to_vec(),
        Some((bucket, key)) => {
            let mut result: Vec<u8> = Vec::new();
            let mut resp = s3.get_object().bucket(bucket).key(key);
            if (start > 0) | (end > 0) {
                resp = resp.set_range(Some(format!(
                    "bytes={}-{}",
                    start,
                    end - 1
                )))
            };
            if requester_pays {
                resp = resp.set_request_payer(Some(RequestPayer::Requester));
            }
            let resp = if anon {
                resp.customize()
                    .await
                    .unwrap()
                    .map_operation(make_unsigned)
                    .unwrap()
                    .send()
                    .await
            } else {
                resp.send().await
            };
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
            result
        }
    }
}

#[pyfunction]
fn s3_cat_ranges<'py>(
    py: Python<'py>, path: Vec<&str>, region: Option<&str>,
    profile: Option<&str>, endpoint_url: Option<&str>, start: Vec<usize>,
    end: Vec<usize>, requester_pays: bool, anon: bool,
) -> &'py PyTuple {
    let mut result: Vec<Vec<u8>> = Vec::with_capacity(path.len());
    let coroutine = async {
        let s3_client = s3(region, profile, endpoint_url).await;
        join_all(path.iter().zip(start).zip(end).map(|((u, st), e)| {
            s3_get_one_range(u, s3_client.clone(), st, e, requester_pays, anon)
        }))
        .await
        .into_iter()
        .map(|out: Vec<u8>| result.push(out))
        .count()
    };
    py.allow_threads(|| RUNTIME.block_on(coroutine));
    PyTuple::new(py, result.iter().map(|r| PyBytes::new(py, &r[..])))
}

async fn gcs() -> TokenManager {
    let cname: &str = "full-control";
    if GCS_TOKEN.lock().unwrap().contains_key(cname) {
        // clone is free since "client" is actually an Arc pointing to real object
        return GCS_TOKEN.lock().unwrap().get(cname).unwrap().clone();
    }
    let tok = TokenManager::new(&[cname]).await.unwrap();
    GCS_TOKEN.lock().unwrap().insert(cname.to_string(), tok.clone());
    tok
}

async fn gcs_get_range(
    path: &str, tok: Option<String>, start: usize, end: usize,
    project: Option<&str>, requester_pays: bool,
) -> Bytes {
    let mut head: HashMap<&str, String> = HashMap::new();
    let mut extra: String = String::new();
    if let Some(tok_str) = tok {
        head.insert("authorization", tok_str);
        if let Some(proj) = project {
            head.insert("x-goog-user-project", proj.to_string());
            if requester_pays {
                extra.extend(format!("&userProject={}", proj).chars());
            }
        };
    }
    let (bucket, key) = path.split_once("/").unwrap();
    // or STORAGE_EMULATOR_HOST env var for testing
    let host = "https://storage.googleapis.com";
    let url = format!(
        "{}/download/storage/v1/b/{}/o/{}?alt=media{}",
        host,
        bucket,
        encode(key),
        extra
    );
    get_url_or(&url[..], start, end, head, &reqwest::Method::GET).await
}

#[pyfunction]
fn gcs_cat_ranges<'py>(
    py: Python<'py>, path: Vec<&str>, start: Vec<usize>, end: Vec<usize>,
    anon: bool, project: Option<&str>, requester_pays: bool,
) -> &'py PyTuple {
    let mut result: Vec<Bytes> = Vec::with_capacity(path.len());
    let coroutine = async {
        let tok: Option<String> = match anon {
            true => None,
            false => Some(gcs().await.token().await.unwrap()),
        };
        join_all(path.iter().zip(start).zip(end).map(|((u, st), e)| {
            gcs_get_range(u, tok.clone(), st, e, project, requester_pays)
        }))
        .await
        .into_iter()
        .map(|out: Bytes| result.push(out))
        .count()
    };
    py.allow_threads(|| RUNTIME.block_on(coroutine));
    PyTuple::new(py, result.iter().map(|r| PyBytes::new(py, &r[..])))
}

/// A Python module implemented in Rust.
#[pymodule]
fn rfsspec(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(cat_ranges, m)?)?;
    m.add_function(wrap_pyfunction!(get, m)?)?;
    m.add_function(wrap_pyfunction!(s3_cat_ranges, m)?)?;
    m.add_function(wrap_pyfunction!(gcs_cat_ranges, m)?)?;
    Ok(())
}
