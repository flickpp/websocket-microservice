use std::net::TcpListener;
use std::process;
use std::thread::sleep;
use std::time;

use ndjsonlogger::{error, info};

mod config;
mod exchange;
mod httpserver;
mod msgreceiver;
mod server;

use config::Config;

fn bind_sockets(
    cfg: &Config,
    ws_bind: &mut Option<TcpListener>,
    msg_bind: &mut Option<TcpListener>,
) -> Result<(), String> {
    if ws_bind.is_none() {
        *ws_bind = match TcpListener::bind(cfg.ws_bind_addr) {
            Ok(l) => Some(l),
            Err(e) => {
                return Err(format!("ws_bind failed - {}", e));
            }
        }
    }

    if msg_bind.is_none() {
        *msg_bind = match TcpListener::bind(cfg.msg_bind_addr) {
            Ok(l) => Some(l),
            Err(e) => {
                return Err(format!("msg_bind failed - {}", e));
            }
        }
    }

    Ok(())
}

fn main() {
    let cfg = match Config::from_env() {
        Ok(c) => c,
        Err(v) => {
            let v = v.iter().map(|s| &s[..]).collect::<Vec<&str>>();
            error!("invalid value(s) in environment variables", {
                [errors = v]
            });
            process::exit(1);
        }
    };

    info!("websocket-ms started", {
        "cfg.websocket_port_bind"        = &cfg.ws_bind_addr.to_string(),
        "cfg.message_port_bind"          = &cfg.msg_bind_addr.to_string(),
        "cfg.ping_period" : u64          = cfg.ping_period.as_secs(),
        "cfg.http_receive_timeout" : u64 = cfg.msg_recv_timeout.as_secs()
    });

    // Loop/sleep trying to bind to ports
    let mut ws_bind: Option<TcpListener> = None;
    let mut msg_bind: Option<TcpListener> = None;
    while let Err(s) = bind_sockets(&cfg, &mut ws_bind, &mut msg_bind) {
        error!("could not bind to socket - retrying after 5 seconds", {
            error = &s
        });

        sleep(time::Duration::from_secs(5));
    }

    server::run_forever(cfg, ws_bind.unwrap(), msg_bind.unwrap());
}
