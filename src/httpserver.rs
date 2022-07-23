use std::sync::Arc;
use std::time;

use async_std::future::timeout;
use async_std::io;
use async_std::net::TcpStream;
use async_std::prelude::*;
use http_types::{Method, Request, Response, StatusCode};
use ndjsonlogger::error;
use random_fast_rng::{FastRng, Random};

use crate::exchange;
use crate::Config;

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum ConnectionStatus {
    KeepAlive,
    Close,
}

pub struct TraceContext {
    trace_id: String,
    span_id: String,
    parent_id: Option<String>,
}

impl TraceContext {
    fn from_header(trace: &str) -> Self {
        // 00-{trace_id}-{parent_id}-00
        let mut trace_id: &str = "";
        let mut parent_id: Option<&str> = None;
        let mut ok = false;
        for (n, p) in trace.split('-').enumerate() {
            match n {
                0 => {
                    if p != "00" {
                        break;
                    }
                }
                1 => {
                    if p.len() != 32 {
                        break;
                    }
                    trace_id = p;
                }
                2 => {
                    if p.len() != 16 {
                        break;
                    }
                    parent_id = Some(p);
                }
                3 => {
                    if p != "00" {
                        break;
                    }

                    ok = true;
                }
                _ => {
                    ok = false;
                    break;
                }
            }
        }

        if ok {
            Self {
                parent_id: parent_id.map(|s| s.to_owned()),
                trace_id: trace_id.to_string(),
                span_id: span_id(),
            }
        } else {
            Self::new()
        }
    }

    fn new() -> Self {
        Self {
            parent_id: None,
            span_id: span_id(),
            trace_id: trace_id(),
        }
    }

    pub fn into_trace_id(self) -> String {
        self.trace_id
    }

    pub fn trace_id(&self) -> &str {
        &self.trace_id
    }

    pub fn span_id(&self) -> &str {
        &self.span_id
    }

    pub fn parent_id(&self) -> Option<&str> {
        self.parent_id.as_ref().map(|s| &s[..])
    }
}

pub async fn accept_websocket<F, Fut>(
    cfg: Arc<Config>,
    exchange: exchange::Exchange,
    mut stream: TcpStream,
    handle_req: F,
) -> Result<(), &'static str>
where
    F: Fn(Arc<Config>, Request, exchange::Exchange, TcpStream, TraceContext) -> Fut + Copy,
    Fut: Future<Output = Result<Response, &'static str>>,
{
    let req = match build_req(stream.clone(), cfg.msg_recv_timeout).await? {
        Some(r) => r,
        None => return Ok(()),
    };
    let trace_ctx = read_trace_ctx(req.iter());
    let trace_id = trace_ctx.trace_id.clone();
    let mut resp = handle_req(cfg, req, exchange, stream.clone(), trace_ctx)
        .await
        .unwrap_or_else(|error| {
            error!("internal server error", { error });
            let mut resp = Response::new(StatusCode::InternalServerError);
            resp.insert_header("X-Error", error);
            resp
        });

    resp.insert_header("X-TraceId", trace_id);
    resp.insert_header("Server", "websocket-microservice");

    // Write the response
    let mut encoder = async_h1::server::Encoder::new(resp, Method::Get);
    io::copy(&mut encoder, &mut stream)
        .await
        .map_err(|_| "couldn't write bytes to stream")?;

    Ok(())
}

pub async fn accept<F, Fut>(
    cfg: Arc<Config>,
    exchange: exchange::Exchange,
    stream: TcpStream,
    handle_req: F,
) -> Result<(), &'static str>
where
    F: Fn(Request, exchange::Exchange, TraceContext) -> Fut + Copy,
    Fut: Future<Output = Result<Response, &'static str>>,
{
    while ConnectionStatus::KeepAlive
        == accept_one(&cfg, exchange.clone(), stream.clone(), handle_req).await?
    {}
    Ok(())
}

async fn build_req(
    mut stream: TcpStream,
    max_time: time::Duration,
) -> Result<Option<Request>, &'static str> {
    let fut = async_h1::server::decode(stream.clone());

    let req = timeout(max_time, fut)
        .await
        .map_err(|_| "timed out waiting for header")?;

    let (req, _) = match req {
        Ok(Some(r)) => r,
        Ok(None) => {
            return Ok(None);
        }
        Err(e) => {
            // Attempt to write error to client
            let code = e.status();
            let mut encoder = async_h1::server::Encoder::new(Response::new(code), Method::Get);
            io::copy(&mut encoder, &mut stream)
                .await
                .map_err(|_| "couldn't write bytes to tcp stream")?;

            return Ok(None);
        }
    };

    Ok(Some(req))
}

async fn accept_one<F, Fut>(
    cfg: &Config,
    exchange: exchange::Exchange,
    mut stream: TcpStream,
    handle_req: F,
) -> Result<ConnectionStatus, &'static str>
where
    F: Fn(Request, exchange::Exchange, TraceContext) -> Fut + Copy,
    Fut: Future<Output = Result<Response, &'static str>>,
{
    let req = match build_req(stream.clone(), cfg.msg_recv_timeout).await? {
        Some(r) => r,
        None => return Ok(ConnectionStatus::Close),
    };
    let trace_ctx = read_trace_ctx(req.iter());

    // Process the request
    let method = req.method();
    let trace_id = trace_ctx.trace_id.clone();
    let mut resp = handle_req(req, exchange, trace_ctx)
        .await
        .unwrap_or_else(|error| {
            error!("internal server error", { error });
            let mut resp = Response::new(StatusCode::InternalServerError);
            resp.insert_header("X-Error", error);
            resp
        });

    resp.insert_header("X-TraceId", trace_id);
    resp.insert_header("Server", "websocket-microservice");

    // Write the response
    let mut encoder = async_h1::server::Encoder::new(resp, method);
    io::copy(&mut encoder, &mut stream)
        .await
        .map_err(|_| "couldn't write bytes to stream")?;

    Ok(ConnectionStatus::KeepAlive)
}

fn span_id() -> String {
    hex::encode(FastRng::new().gen::<[u8; 8]>())
}

fn trace_id() -> String {
    hex::encode(FastRng::new().gen::<[u8; 16]>())
}

fn read_trace_ctx(headers: http_types::headers::Iter) -> TraceContext {
    for (key, value) in headers {
        for v in value {
            if key.as_str().eq_ignore_ascii_case("traceparent") {
                return TraceContext::from_header(v.as_str());
            }
        }
    }

    TraceContext::new()
}
