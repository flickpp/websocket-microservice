use std::sync::Arc;
use std::time;

use async_std::channel::{unbounded, Receiver, Sender};
use async_std::future::timeout;
use async_std::net::{TcpListener, TcpStream};
use async_std::task;
use async_tungstenite::tungstenite::Message;
use futures::future::{select, Either};
use futures::{SinkExt, StreamExt};
use http_types::{Method, Request, Response, StatusCode};
use ndjsonlogger::{debug, error, info};
use random_fast_rng::{FastRng, Random};
use sha1::{Digest, Sha1};

use crate::exchange::{self, WebsocketMessage, WebsocketResponse};
use crate::httpserver;
use crate::msgreceiver;
use crate::Config;

pub fn run_forever(
    cfg: Config,
    ws_listener: std::net::TcpListener,
    msg_listener: std::net::TcpListener,
) {
    let cfg = Arc::new(cfg);
    let exchange = exchange::Exchange::new(cfg.ping_period);
    let ws_listener = TcpListener::from(ws_listener);
    let msg_listener = TcpListener::from(msg_listener);

    task::block_on(run(cfg, exchange, ws_listener, msg_listener));
}

async fn run(
    cfg: Arc<Config>,
    exchange: exchange::Exchange,
    ws_listener: TcpListener,
    msg_listener: TcpListener,
) {
    // Run the msg receiver
    task::spawn(msgreceiver::run(
        cfg.clone(),
        exchange.clone(),
        msg_listener,
    ));

    // Run websocket server forever
    run_websocket(cfg, exchange, ws_listener).await
}

async fn run_websocket(cfg: Arc<Config>, exchange: exchange::Exchange, listener: TcpListener) {
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
    if let Err(error) = httpserver::accept_websocket(cfg, exchange, stream, handle_req).await {
        error!("tcp stream closed", { error, conn = "websocket" });
    }
}

async fn handle_req(
    cfg: Arc<Config>,
    req: Request,
    exchange: exchange::Exchange,
    stream: TcpStream,
    trace_ctx: httpserver::TraceContext,
) -> Result<Response, &'static str> {
    if req.method() != Method::Get {
        let mut resp = Response::new(StatusCode::BadRequest);
        resp.insert_header("X-Error", "Only GET method valid for websocket streams");
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

    let session_id = match session_id {
        None => {
            let mut resp = Response::new(StatusCode::BadRequest);
            resp.insert_header("X-Error", "session_id is required url param");
            return Ok(resp);
        }
        Some(s) => s,
    };

    let headers = read_headers(&req);
    let is_websocket = match (headers.upgrade, headers.connection) {
        (Some(upgrade), Some(connection)) => {
            if upgrade.eq_ignore_ascii_case("websocket")
                && connection.eq_ignore_ascii_case("upgrade")
            {
                Ok(())
            } else {
                Err("upgrade must equal websocket and connection must equal upgrade")
            }
        }
        (Some(_), None) => Err("missing connection header"),
        (None, Some(_)) => Err("missing upgrade header"),
        (None, None) => Err("missing connection and upgrade header"),
    };

    if let Err(s) = is_websocket {
        let mut resp = Response::new(StatusCode::BadRequest);
        resp.insert_header("X-Error", s);
        return Ok(resp);
    }

    let sec_websocket_accept = match headers.sec_websocket_key {
        None => {
            let mut resp = Response::new(StatusCode::BadRequest);
            resp.insert_header("X-Error", "missing Sec-Websocket-Key header");
            return Ok(resp);
        }
        Some(s) => compute_sec_websocket_accept(s),
    };

    let (sender, receiver) = unbounded();
    exchange
        .register_stream(
            &session_id,
            login_id.as_ref(),
            sender.clone(),
            cfg.ping_period,
        )
        .await;

    info!("websocket connection established", {
        session_id             = &session_id,
        login_id: Option<&str> = login_id.as_ref().map(|s| &s[..]),
        trace_id               = trace_ctx.trace_id()
    });
    task::spawn(handle_websocket(
        stream,
        session_id,
        receiver,
        trace_ctx.into_trace_id(),
    ));

    let mut resp = Response::new(StatusCode::SwitchingProtocols);
    resp.insert_header("Upgrade", "Websocket");
    resp.insert_header("Connection", "Upgrade");
    resp.insert_header("Sec-Websocket-Accept", sec_websocket_accept);
    Ok(resp)
}

#[derive(Default)]
struct Headers<'r> {
    upgrade: Option<&'r str>,
    connection: Option<&'r str>,
    sec_websocket_key: Option<&'r str>,
    sec_websocket_version: Option<&'r str>,
    traceparent: Option<&'r str>,
}

fn read_headers(req: &Request) -> Headers<'_> {
    let mut headers = Headers::default();
    for (name, values) in req.iter() {
        for value in values {
            if name.as_str().eq_ignore_ascii_case("upgrade") {
                headers.upgrade = Some(value.as_str());
            } else if name.as_str().eq_ignore_ascii_case("connection") {
                headers.connection = Some(value.as_str());
            } else if name.as_str().eq_ignore_ascii_case("sec-websocket-key") {
                headers.sec_websocket_key = Some(value.as_str());
            } else if name.as_str().eq_ignore_ascii_case("sec_websocket_version") {
                headers.sec_websocket_version = Some(value.as_str());
            } else if name.as_str().eq_ignore_ascii_case("traceparent") {
                headers.traceparent = Some(value.as_str());
            }
        }
    }

    headers
}

fn compute_sec_websocket_accept(sec_websocket_key: &str) -> String {
    let mut hasher = Sha1::new();
    hasher.update(sec_websocket_key.as_bytes());
    hasher.update("258EAFA5-E914-47DA-95CA-C5AB0DC85B11".as_bytes());
    base64::encode(hasher.finalize())
}

async fn handle_websocket(
    stream: TcpStream,
    session_id: String,
    recv: Receiver<WebsocketMessage>,
    trace_id: String,
) {
    let mut resp_sender: Option<Sender<WebsocketResponse>> = None;
    let mut error: Option<&'static str> = None;
    let mut send_close = false;
    let role = async_tungstenite::tungstenite::protocol::Role::Server;
    let websocket = async_tungstenite::WebSocketStream::from_raw_socket(stream, role, None).await;
    let (mut ws_stream_send, mut ws_stream_recv) = websocket.split();
    let mut pending_pings = PendingPings::new();

    let mut ws_msg_fut = ws_stream_recv.next();
    let mut recv_fut = recv.recv();

    let close_reason = loop {
        if let Some(resp_sender) = resp_sender.take() {
            resp_sender
                .send(WebsocketResponse {
                    session_id: session_id.clone(),
                    error: error.take(),
                })
                .await
                // http request has gone away - do nothing
                .unwrap_or(());
        }

        if send_close {
            // Send a close message
            if ws_stream_send.send(Message::Close(None)).await.is_err() {
                break "client has closed connection";
            }

            let reason = loop {
                match timeout(time::Duration::from_secs(30), ws_msg_fut).await {
                    // timeout
                    Err(_) => break "timed out waiting for client to return close",
                    // recv close
                    Ok(Some(Ok(Message::Close(_)))) => break "connection closed gracefull",
                    // stream closed
                    Ok(None) => break "client has closed connection closed",
                    Ok(Some(Err(_))) => break "client has closed connection",
                    Ok(Some(Ok(_))) => {
                        ws_msg_fut = ws_stream_recv.next();
                    }
                }
            };

            break reason;
        }

        match select(ws_msg_fut, recv_fut).await {
            Either::Left((msg, recv_cont)) => {
                let ws_msg = msg
                    .transpose()
                    .map_err(|_| "client has closed connection")
                    .and_then(|msg| msg.ok_or("client has closed connection"));

                match ws_msg {
                    Err(s) => break s,
                    Ok(Message::Ping(data)) => {
                        if ws_stream_send.send(Message::Pong(data)).await.is_err() {
                            break "client has closed connection";
                        }
                    }
                    Ok(Message::Pong(data)) => {
                        debug!("received pong", {
                            session_id = &session_id,
                            [data: u8  = data]
                        });
                        // check our pings
                        if pending_pings.recv_pong(&data).is_none() {
                            // misbehaving client
                            send_close = true;
                        }
                    }
                    Ok(Message::Close(_)) => {
                        ws_stream_send
                            .send(Message::Close(None))
                            .await
                            .unwrap_or(());
                        break "client sent close frame";
                    }
                    _ => {
                        // misbehaving client
                        break "received invalid message from client";
                    }
                }
                ws_msg_fut = ws_stream_recv.next();
                recv_fut = recv_cont;
            }
            Either::Right((recv_msg, ws_msg_cont)) => {
                match recv_msg {
                    Err(_) => {
                        // exchange has removed us
                        send_close = true;
                    }
                    Ok(WebsocketMessage::Ping) => {
                        // Send a ping to client
                        if pending_pings.num_pending() >= 3 {
                            debug!("closing due to no ping response", {
                                session_id = &session_id
                            });
                            send_close = true;
                        } else {
                            let data = pending_pings.new_ping();
                            debug!("sending ping", {
                                session_id = &session_id,
                                [data: u8  = data]
                            });
                            if ws_stream_send.send(Message::Ping(data)).await.is_err() {
                                error = Some("client stream has closed");
                            }
                        }
                    }
                    Ok(WebsocketMessage::Close) => {
                        send_close = true;
                    }
                    Ok(WebsocketMessage::String((data, digest, sender))) => {
                        // Send string message
                        info!("sending string message", {
                            session_id = &session_id,
                            trace_id   = &trace_id,
                            digest     = &digest[..]
                        });
                        resp_sender = Some(sender);
                        if ws_stream_send.send(Message::Text(data)).await.is_err() {
                            error = Some("client stream has closed");
                        }
                    }
                    Ok(WebsocketMessage::Binary((data, digest, sender))) => {
                        // Send binary message
                        info!("sending binary message", {
                            session_id = &session_id,
                            trace_id   = &trace_id,
                            digest     = &digest[..]
                        });
                        resp_sender = Some(sender);
                        if ws_stream_send.send(Message::Binary(data)).await.is_err() {
                            error = Some("client stream has closed");
                        }
                    }
                }
                recv_fut = recv.recv();
                ws_msg_fut = ws_msg_cont;
            }
        }
    };

    info!("websocket closing", {
        session_id = &session_id,
        trace_id = &trace_id,
        close_reason
    });
}

#[derive(Default)]
struct PendingPings {
    pending: Vec<[u8; 8]>,
}

impl PendingPings {
    fn new() -> Self {
        Self::default()
    }

    fn new_ping(&mut self) -> Vec<u8> {
        let data = FastRng::new().gen::<[u8; 8]>();
        let mut data_vec = vec![];
        data_vec.extend(&data);
        self.pending.push(data);
        data_vec
    }

    fn recv_pong(&mut self, data: &[u8]) -> Option<[u8; 8]> {
        let mut ind: Option<usize> = None;

        for (n, v) in self.pending.iter().enumerate() {
            if v == data {
                ind = Some(n);
                break;
            }
        }

        ind.map(|ind| self.pending.remove(ind))
    }

    fn num_pending(&self) -> usize {
        self.pending.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_sec_websocket_accept() {
        let key = "dGhlIHNhbXBsZSBub25jZQ==";
        assert_eq!(
            compute_sec_websocket_accept(key),
            "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="
        );
    }
}
