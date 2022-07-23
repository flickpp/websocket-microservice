use std::sync::Arc;
use std::time;

use std::collections::{HashMap, HashSet};

use async_std::channel::{unbounded, Receiver, Sender};
use async_std::stream;
use async_std::sync::Mutex;
use async_std::task::{self, sleep};
use futures::future::{select, Either};
use futures::StreamExt;
use sha1::{Digest, Sha1};

#[derive(Clone)]
pub struct Exchange {
    inner: Arc<Mutex<Inner>>,
    pingq_send: Sender<(Sender<WebsocketMessage>, String, time::Instant)>,
}

#[derive(Default)]
struct Inner {
    streams: HashMap<String, Sender<WebsocketMessage>>,
    login_map: HashMap<String, HashSet<String>>,
}

pub enum BroadcastType {
    SessionId(String),
    LoginId(String),
}

pub struct WebsocketResponse {
    pub session_id: String,
    pub error: Option<&'static str>,
}

#[derive(Clone)]
pub enum WebsocketMessage {
    Ping,
    Close,
    String((String, String, Sender<WebsocketResponse>)),
    Binary((Vec<u8>, String, Sender<WebsocketResponse>)),
}

#[derive(serde::Serialize, Default)]
pub struct ResponseBody {
    pub session_ids: Vec<String>,
    pub digest: String,
}

impl Exchange {
    pub fn new(ping_period: time::Duration) -> Self {
        let (pingq_send, pingq_recv) = unbounded();
        let slf = Self {
            inner: Arc::new(Mutex::new(Inner::default())),
            pingq_send,
        };

        task::spawn(send_pings(slf.clone(), pingq_recv, ping_period));

        slf
    }

    pub async fn send_binary(&self, broadcast_type: BroadcastType, val: Vec<u8>) -> ResponseBody {
        self.inner
            .lock()
            .await
            .send_binary(broadcast_type, val)
            .await
    }

    pub async fn send_string(&self, broadcast_type: BroadcastType, val: String) -> ResponseBody {
        self.inner
            .lock()
            .await
            .send_string(broadcast_type, val)
            .await
    }

    pub async fn register_stream(
        &self,
        session_id: &str,
        login_id: Option<&String>,
        sender: Sender<WebsocketMessage>,
        ping_period: time::Duration,
    ) {
        let ping_time = time::Instant::now() + ping_period;
        self.pingq_send
            .send((sender.clone(), session_id.to_string(), ping_time))
            .await
            .expect("ping sender task has crashed");

        self.inner
            .lock()
            .await
            .register_stream(session_id, login_id, sender)
            .await
    }

    async fn remove_session_id(&self, session_id: &str) {
        self.inner.lock().await.remove_session_id(session_id);
    }
}

fn compute_digest(val: &[u8]) -> String {
    let mut hasher = Sha1::new();
    hasher.update(val);
    hex::encode(&hasher.finalize()[..12])
}

impl Inner {
    async fn register_stream(
        &mut self,
        session_id: &str,
        login_id: Option<&String>,
        sender: Sender<WebsocketMessage>,
    ) {
        if let Some(sender) = self.streams.remove(session_id) {
            // Send Close to existing connection with this session_id
            sender.send(WebsocketMessage::Close).await.unwrap_or(());
        }
        self.streams.insert(session_id.to_string(), sender);
        if let Some(login_id) = login_id {
            let set = match self.login_map.get_mut(login_id) {
                Some(set) => set,
                None => {
                    self.login_map.insert(login_id.to_owned(), HashSet::new());
                    self.login_map.get_mut(login_id).unwrap()
                }
            };

            set.insert(session_id.to_string());
        }
    }

    fn remove_session_id(&mut self, session_id: &str) {
        self.streams.remove(session_id);
        let mut to_remove = vec![];
        for (login_id, session_id_set) in self.login_map.iter_mut() {
            session_id_set.remove(session_id);
            if session_id_set.is_empty() {
                to_remove.push(login_id.to_owned());
            }
        }

        for login_id in to_remove {
            self.login_map.remove(&login_id);
        }
    }

    async fn send_string(&self, broadcast_type: BroadcastType, val: String) -> ResponseBody {
        let (resp_send, resp_recv) = unbounded();
        let digest = compute_digest(val.as_bytes());
        let msg = WebsocketMessage::String((val, digest, resp_send));
        self.send(broadcast_type, msg, resp_recv).await
    }

    async fn send_binary(&self, broadcast_type: BroadcastType, val: Vec<u8>) -> ResponseBody {
        let (resp_send, resp_recv) = unbounded();
        let digest = compute_digest(&val);
        let msg = WebsocketMessage::Binary((val, digest, resp_send));
        self.send(broadcast_type, msg, resp_recv).await
    }

    async fn send(
        &self,
        broadcast_type: BroadcastType,
        msg: WebsocketMessage,
        resp_recv: Receiver<WebsocketResponse>,
    ) -> ResponseBody {
        let mut num_msgs = 0;
        for sender in self.get_senders(broadcast_type) {
            if sender.send(msg.clone()).await.is_ok() {
                num_msgs += 1;
            }
        }

        let mut recv_fut = resp_recv.recv();
        let mut session_ids = vec![];
        let mut ticker = stream::interval(time::Duration::from_secs(10));
        let mut timeout = ticker.next();
        while num_msgs > 0 {
            match select(timeout, recv_fut).await {
                // timeout
                Either::Left(_) => break,
                Either::Right((msg, timeout_continue)) => {
                    match msg {
                        // No senders left
                        Err(_) => break,
                        Ok(resp) => {
                            if resp.error.is_none() {
                                // Message send succesfully
                                session_ids.push(resp.session_id);
                            }

                            num_msgs -= 1;
                        }
                    }
                    timeout = timeout_continue;
                    recv_fut = resp_recv.recv();
                }
            }
        }
        ResponseBody {
            session_ids,
            digest: match msg {
                WebsocketMessage::String((_, d, _)) => d,
                WebsocketMessage::Binary((_, d, _)) => d,
                WebsocketMessage::Ping => unreachable!("should not send ping message manually!"),
                WebsocketMessage::Close => unreachable!("should not send close message manually!"),
            },
        }
    }

    fn get_senders(&self, broadcast_type: BroadcastType) -> Vec<Sender<WebsocketMessage>> {
        let mut senders = vec![];

        match broadcast_type {
            BroadcastType::LoginId(login_id) => {
                if let Some(set) = self.get_session_ids(&login_id) {
                    for session_id in set.iter() {
                        if let Some(sender) = self.get_sender(session_id) {
                            senders.push(sender);
                        }
                    }
                }
            }
            BroadcastType::SessionId(session_id) => {
                if let Some(sender) = self.get_sender(&session_id) {
                    senders.push(sender);
                }
            }
        }

        senders
    }

    fn get_sender(&self, session_id: &str) -> Option<Sender<WebsocketMessage>> {
        self.streams.get(session_id).map(|s| s.to_owned())
    }

    fn get_session_ids<'s>(&'s self, login_id: &str) -> Option<&'s HashSet<String>> {
        self.login_map.get(login_id)
    }
}

async fn send_pings(
    exchange: Exchange,
    pingq_recv: Receiver<(Sender<WebsocketMessage>, String, time::Instant)>,
    ping_period: time::Duration,
) {
    loop {
        let loop_time = time::Instant::now();
        let (sender, session_id, ping_time) = match pingq_recv.recv().await {
            Ok(r) => r,
            Err(_) => break,
        };

        if ping_time > loop_time {
            sleep(ping_time - loop_time).await;
        }

        if sender.send(WebsocketMessage::Ping).await.is_err() {
            // can't send means receiver has dropped
            // this means the connection is no longer active
            exchange.remove_session_id(&session_id).await;
        } else {
            exchange
                .pingq_send
                .send((sender, session_id, ping_time + ping_period))
                .await
                // We can't fail as pingq_recv is cleary in scope
                .unwrap();
        }
    }
}
