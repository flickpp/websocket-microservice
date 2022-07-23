use std::env;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::str::FromStr;
use std::time;

pub struct Config {
    pub ws_bind_addr: SocketAddr,
    pub msg_bind_addr: SocketAddr,
    pub msg_recv_timeout: time::Duration,
    pub ping_period: time::Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            ws_bind_addr: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 8080)),
            msg_bind_addr: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 8081)),
            msg_recv_timeout: time::Duration::from_secs(30),
            ping_period: time::Duration::from_secs(30),
        }
    }
}

impl Config {
    pub fn from_env() -> Result<Self, Vec<String>> {
        let mut cfg = Self::default();
        let mut errs = vec![];

        for (key, val) in env::vars() {
            match key.as_ref() {
                "WEBSOCKET_MICROSERVICE_WEBSOCKET_BIND" => match SocketAddr::from_str(&val) {
                    Ok(ws_bind_addr) => {
                        cfg.ws_bind_addr = ws_bind_addr;
                    }
                    Err(e) => {
                        errs.push(format!("invalid websocket port bind [{}]", e));
                    }
                },
                "WEBSOCKET_MICROSERVICE_MESSAGE_BIND" => match SocketAddr::from_str(&val) {
                    Ok(ws_bind_addr) => {
                        cfg.msg_bind_addr = ws_bind_addr;
                    }
                    Err(e) => {
                        errs.push(format!("invalid message port bind [{}]", e));
                    }
                },
                "WEBSOCKET_MICROSERVICE_HTTP_RECEIVE_TIMEOUT" => match val.parse::<u64>() {
                    Ok(timeout) => {
                        cfg.msg_recv_timeout = time::Duration::from_secs(timeout);
                    }
                    Err(e) => {
                        errs.push(format!("invalid http receive timeout [{}]", e));
                    }
                },
                "WEBSOCKET_MICROSERVICE_PING_PERIOD" => match val.parse::<u64>() {
                    Ok(timeout) => {
                        cfg.ping_period = time::Duration::from_secs(timeout);
                    }
                    Err(e) => {
                        errs.push(format!("invalid ping period [{}]", e));
                    }
                },
                _ => {}
            }
        }

        if errs.is_empty() {
            Ok(cfg)
        } else {
            Err(errs)
        }
    }
}
