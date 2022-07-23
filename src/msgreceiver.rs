use std::str::FromStr;
use std::sync::Arc;

use async_std::net::{TcpListener, TcpStream};
use async_std::prelude::*;
use async_std::task;

use http_types::{Body, Method, Mime, Request, Response, StatusCode};
use ndjsonlogger::{debug, error, info};

use crate::exchange::{self, BroadcastType};
use crate::httpserver;
use crate::Config;

pub async fn run(cfg: Arc<Config>, exchange: exchange::Exchange, listener: TcpListener) {
    while let Some(stream) = listener.incoming().next().await {
        match stream {
            Ok(s) => {
                task::spawn(handle_stream(cfg.clone(), exchange.clone(), s));
            }
            Err(e) => {
                debug!("error in tcp stream", { error = &format!("{}", e) });
            }
        }
    }
}

async fn handle_stream(cfg: Arc<Config>, exchange: exchange::Exchange, stream: TcpStream) {
    if let Err(error) = httpserver::accept(cfg, exchange, stream, handle_req).await {
        error!("tcp stream closed", { error, conn = "msgreceiver" });
    }
}

enum MsgType {
    String(String),
    Binary(Vec<u8>),
}

async fn handle_req(
    mut req: Request,
    exchange: exchange::Exchange,
    trace_ctx: httpserver::TraceContext,
) -> Result<Response, &'static str> {
    if req.method() != Method::Post {
        let mut resp = Response::new(StatusCode::BadRequest);
        resp.insert_header("X-Error", "only POST method valid");
        return Ok(resp);
    }

    let mut session_id: Option<String> = None;
    let mut login_id: Option<String> = None;

    for (key, value) in req.url().query_pairs() {
        if key == "session_id" {
            session_id = Some(value.to_string());
        } else if key == "login_id" {
            login_id = Some(value.to_string());
        }
    }

    let broadcast_type = match (session_id, login_id) {
        (Some(session_id), None) => BroadcastType::SessionId(session_id),
        (None, Some(login_id)) => BroadcastType::LoginId(login_id),
        (Some(_), Some(_)) => {
            let mut resp = Response::new(StatusCode::BadRequest);
            resp.insert_header(
                "X-Error",
                "exactly one of session_id or login_id url param required",
            );
            return Ok(resp);
        }
        (None, None) => {
            let mut resp = Response::new(StatusCode::BadRequest);
            resp.insert_header(
                "X-Error",
                "exactly one of session_id or login_id url param required",
            );
            return Ok(resp);
        }
    };

    let msg_type_res = match req.url().path() {
        "/binary" => req.body_bytes().await.map(MsgType::Binary),
        "/string" => req.body_string().await.map(MsgType::String),
        _ => {
            let mut resp = Response::new(StatusCode::BadRequest);
            resp.insert_header(
                "X-Error",
                "unrecognised path /binary and /string are valid paths",
            );
            return Ok(resp);
        }
    };

    let resp_body = match msg_type_res {
        Err(e) => {
            let mut resp = Response::new(e.status());
            resp.insert_header("X-Error", "couldn't read body");
            return Ok(resp);
        }
        Ok(MsgType::Binary(v)) => exchange.send_binary(broadcast_type, v).await,
        Ok(MsgType::String(s)) => exchange.send_string(broadcast_type, s).await,
    };

    info!("sent messages to clients", {
        trace_id                = trace_ctx.trace_id(),
        span_id                 = trace_ctx.span_id(),
        parent_id: Option<&str> = trace_ctx.parent_id(),
        msg_type                = &req.url().path()[1..],
        num_clients: usize      = resp_body.session_ids.len(),
        digest                  = &resp_body.digest[..]
    });

    let mut resp = Response::new(StatusCode::Ok);
    resp.set_content_type(Mime::from_str("application/json").unwrap());
    resp.set_body(Body::from_json(&resp_body).expect("couldn't serialize response body to json"));

    Ok(resp)
}
