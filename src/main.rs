use actix::prelude::*;
use actix_web::error::{Error as ActixError, ErrorUnauthorized};
use actix_web::http::header;
use actix_web::ws;
use actix_web::{server, App, HttpRequest, HttpResponse};
use futures::{compat::*, prelude::*};
use std::collections::VecDeque;
use structopt::StructOpt;

#[macro_use]
extern crate log;
extern crate chrono;
extern crate env_logger;
mod logging;

mod query_string;
use query_string::QueryString;
use std::collections::HashMap;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

fn ws_index(
    req: HttpRequest,
    stem_url: String,
    caging_url: String,
) -> Result<HttpResponse, ActixError> {
    let ws_proxy = WsProxy::with_request(req.clone(), stem_url.clone(), caging_url.clone())?;
    ws::start(&req, ws_proxy)
}

#[derive(PartialEq, Eq, Hash, Clone, Debug)]
pub struct ModelVersion {
    model: String,
    version: String,
}

pub struct WsProxy {
    stem_url: String,
    caging_url: String,
    query_string: QueryString,
    auth_header: header::HeaderValue,
    queue: VecDeque<ws::Message>,
    stem_connection: Connection,
    model_version: ModelVersion,
    pow_model_version: ModelVersion,
    current_model_version: ModelVersion,
    caging_connection: Connection,
    current_vocab: Option<String>,
    switch_lock: bool,
    temp_duration: f32,
    offset_duration: f32,
    stop: bool,
    caging_stopped: bool,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct ToCaging {
    streaming_response: StreamingResponse,
    vocab: String,
}

// TODO: make this an actor, that way I can carry more info (like what model was used) - or, make stem OPTIONALLY return the model used with results
#[derive(Default)]
pub struct Connection {
    reader: Option<ws::ClientReader>,
    writer: Option<ws::ClientWriter>,
}

impl WsProxy {
    pub fn with_request(
        req: HttpRequest,
        stem_url: String,
        caging_url: String,
    ) -> Result<Self, ActixError> {
        match req.headers().get(header::AUTHORIZATION) {
            Some(auth_header) => {
                let query_string = QueryString::from_str(req.query_string())?;

                let model = query_string
                    .pairs
                    .get("model")
                    .unwrap_or(&"general".to_string())
                    .clone();
                let version = query_string
                    .pairs
                    .get("version")
                    .unwrap_or(&"latest".to_string())
                    .clone();

                let model_version = ModelVersion {
                    model: model.clone(),
                    version: version.clone(),
                };
                let pow_model_version = ModelVersion {
                    model: model.clone(),
                    version: "pow".to_string(),
                };

                let stem_connection = Default::default();

                let current_model_version = model_version.clone();

                let caging_connection = Default::default();
                let current_vocab = None;

                Ok(Self {
                    stem_url,
                    caging_url,
                    query_string,
                    auth_header: auth_header.clone(),
                    queue: VecDeque::new(),
                    stem_connection,
                    model_version,
                    pow_model_version,
                    current_model_version,
                    caging_connection,
                    current_vocab,
                    switch_lock: false,
                    temp_duration: 0.0,
                    offset_duration: 0.0,
                    stop: false,
                    caging_stopped: false,
                })
            }
            None => {
                error!("invalid or missing credentials");
                Err(ErrorUnauthorized("Invalid or missing credentials."))
            }
        }
    }
}

impl Actor for WsProxy {
    type Context = ws::WebsocketContext<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        debug!("WsProxy Actor has started");

        ctx.address().do_send(ConnectToStem {
            model_version: self.model_version.clone(),
        });

        // open connection to the caging server
        connect_to_caging_wrapper(self.caging_url.clone(), self, ctx);
    }

    fn stopping(&mut self, ctx: &mut Self::Context) -> Running {
        debug!("WsProxy Actor is stopping maybe");

        if !self.stop {
            info!("WsProxy will not stop (stop is false)");
            return Running::Continue;
        }

        if !self.caging_stopped {
            info!("WsProxy will not stop (caging_stopped is false)");
            return Running::Continue;
        }

        ctx.close(None);

        Running::Stop
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        debug!("WsProxy Actor has stopped");
    }
}

impl StreamHandler<ws::Message, ws::ProtocolError> for WsProxy {
    fn handle(&mut self, msg: ws::Message, ctx: &mut Self::Context) {
        trace!("message from client");

        match msg {
            ws::Message::Text(text) => {
                self.queue.push_back(actix_web::ws::Message::Text(text));
                ctx.address().do_send(ProcessQueue);
            }
            ws::Message::Binary(binary) => {
                self.queue.push_back(actix_web::ws::Message::Binary(binary));
                ctx.address().do_send(ProcessQueue);
            }
            ws::Message::Close(reason) => {
                warn!("close from client with reason: {:?}", reason);
                ctx.close(reason.clone());
                ctx.stop();
            }
            _ => (),
        }
    }

    fn error(&mut self, err: ws::ProtocolError, ctx: &mut Self::Context) -> Running {
        error!("client stream got the error {:?} .", err);
        ctx.close(None);
        Running::Stop
    }
}

struct FromStem {
    inner: ws::Message,
    model_version: ModelVersion,
}

impl Message for FromStem {
    type Result = ();
}

impl StreamHandler<FromStem, ws::ProtocolError> for WsProxy {
    fn handle(&mut self, msg: FromStem, ctx: &mut Self::Context) {
        if let Some(writer) = self.caging_connection.writer.as_mut() {
            match msg.inner {
                ws::Message::Text(text) => {
                    debug!(
                        "text from stem {:?}, to caging: {}",
                        msg.model_version, text
                    );
                    let streaming_response: Result<StreamingResponse, serde_json::error::Error> =
                        serde_json::from_str(&text);
                    match streaming_response {
                        Ok(mut streaming_response) => {
                            if streaming_response.is_final {
                                self.temp_duration += streaming_response.duration;
                            }

                            // adjust timings
                            streaming_response.start += self.offset_duration;
                            for alternative in &mut streaming_response.channel.alternatives {
                                for mut word in &mut alternative.words {
                                    word.start += self.offset_duration;
                                    word.end += self.offset_duration;
                                }
                            }

                            match &self.current_vocab {
                                Some(current_vocab) => {
                                    let to_caging = serde_json::to_string(&ToCaging {
                                        streaming_response: streaming_response.clone(),
                                        vocab: current_vocab.clone(),
                                    })
                                    .unwrap(); // TODO: check unwrap
                                    writer.text(to_caging);
                                }
                                None => {
                                    let to_caging = serde_json::to_string(&ToCaging {
                                        streaming_response: streaming_response.clone(),
                                        vocab: "".to_string(),
                                    })
                                    .unwrap(); // TODO: check unwrap
                                    writer.text(to_caging);
                                }
                            }
                        }
                        Err(_) => {
                            writer.text(text);
                        }
                    }
                }
                ws::Message::Binary(binary) => {
                    warn!(
                        "binary from stem {:?}, for some reason with length: {}",
                        msg.model_version,
                        binary.len()
                    );
                }
                ws::Message::Close(reason) => {
                    info!(
                        "close from stem with reason: {:?} for {:?}",
                        msg.model_version, reason
                    );

                    if !self.stop {
                        info!("will try to connect to a new stem (current one is {:?}), and upon connection try to process the queue of client messages", msg.model_version);
                        ctx.address().do_send(ConnectToStem {
                            model_version: self.current_model_version.clone(),
                        });
                    } else {
                        info!("will not try to connect to a new stem, because we are stopping");
                        ctx.address().do_send(CloseCaging);
                    }

                    // update the offset duration
                    self.offset_duration += self.temp_duration;
                    self.temp_duration = 0.0;
                }
                _ => {}
            }
        } else {
            error!("connection to caging server not up"); // TODO: reconnect ?
        }
    }

    fn error(&mut self, err: ws::ProtocolError, ctx: &mut Self::Context) -> Running {
        error!("stem stream got the error {:?} .", err);
        ctx.close(None);
        Running::Stop
    }
}

struct FromCaging(ws::Message);

impl FromCaging {
    pub fn into_inner(self) -> ws::Message {
        self.0
    }
}

impl Message for FromCaging {
    type Result = ();
}

impl StreamHandler<FromCaging, ws::ProtocolError> for WsProxy {
    fn handle(&mut self, msg: FromCaging, ctx: &mut Self::Context) {
        match msg.into_inner() {
            ws::Message::Text(text) => {
                trace!("text from caging, to client: {}", text);
                ctx.text(text);
            }
            ws::Message::Binary(binary) => {
                trace!(
                    "binary from caging, to client with length: {}",
                    binary.len()
                );
                ctx.binary(binary);
            }
            ws::Message::Close(reason) => {
                info!("close from caging with reason: {:?}", reason);
                self.caging_stopped = true;
                ctx.stop();
            }
            _ => {}
        }
    }

    fn error(&mut self, err: ws::ProtocolError, ctx: &mut Self::Context) -> Running {
        error!("caging stream got the error {:?} .", err);
        ctx.close(None);
        Running::Stop
    }
}

struct ConnectToStem {
    model_version: ModelVersion,
}

impl Message for ConnectToStem {
    type Result = ();
}

impl Handler<ConnectToStem> for WsProxy {
    type Result = ();

    fn handle(&mut self, msg: ConnectToStem, ctx: &mut Self::Context) -> Self::Result {
        connect_to_stem_wrapper(
            self.stem_url.clone(),
            self.query_string.clone(),
            msg.model_version,
            self.auth_header.clone(),
            self,
            ctx,
        )
    }
}

struct ProcessQueue;

impl Message for ProcessQueue {
    type Result = ();
}

impl Handler<ProcessQueue> for WsProxy {
    type Result = ();

    fn handle(&mut self, _msg: ProcessQueue, _ctx: &mut Self::Context) -> Self::Result {
        if let Some(writer) = self.stem_connection.writer.as_mut() {
            while !self.queue.is_empty() && !self.switch_lock {
                match self.queue.pop_front().unwrap() {
                    ws::Message::Text(text) => {
                        if text == "" {
                            self.current_vocab = None;
                            self.current_model_version = self.model_version.clone();
                        } else {
                            self.current_vocab = Some(text);
                            self.current_model_version = self.pow_model_version.clone();
                        }

                        info!("dequeue'ing text message from client, sending empty byte to stem, activating the switch lock, and using the new model_version: {:?}", self.current_model_version);

                        let empty: Vec<u8> = Vec::new();
                        writer.binary(empty);
                        self.switch_lock = true;
                    }
                    ws::Message::Binary(binary) => {
                        info!("dequeue'ing binary message from client, sending to stem");
                        if binary.is_empty() {
                            info!("client sent empty binary, time to stop things");
                            self.stop = true;
                        }
                        writer.binary(binary);
                    }
                    _ => (),
                }
            }
        }
    }
}

struct StemConnectionSuccessful;

impl Message for StemConnectionSuccessful {
    type Result = ();
}

impl Handler<StemConnectionSuccessful> for WsProxy {
    type Result = ();

    fn handle(&mut self, _msg: StemConnectionSuccessful, ctx: &mut Self::Context) -> Self::Result {
        info!("connection to stem successful, will set the switch lock to false and process any messages which may be in the client message queue");
        self.switch_lock = false;
        ctx.address().do_send(ProcessQueue);
    }
}

struct CloseCaging;

impl Message for CloseCaging {
    type Result = ();
}

impl Handler<CloseCaging> for WsProxy {
    type Result = ();

    fn handle(&mut self, _msg: CloseCaging, _ctx: &mut Self::Context) -> Self::Result {
        info!("closing caging now");
        if let Some(writer) = self.caging_connection.writer.as_mut() {
            let empty: Vec<u8> = Vec::new();
            writer.binary(empty);
        }
    }
}

pub fn connect_to_stem_wrapper(
    url: String,
    query_string: QueryString,
    model_version: ModelVersion,
    auth_header: header::HeaderValue,
    proxy: &mut WsProxy,
    ctx: &mut ws::WebsocketContext<WsProxy>,
) {
    let mut query = query_string.clone();
    query.push("model", &model_version.clone().model);
    query.push("version", &model_version.clone().version);
    let query = query.to_str().unwrap_or_default();
    let url = format!("{}?{}", url, query);
    let cloned_model_version = model_version.clone();

    debug!("attempting to connect to url: {}", url.clone());

    connect_to_stem(url.clone(), auth_header.clone())
        .into_actor(proxy)
        .map(move |(reader, writer), act, ctx| {
            act.stem_connection = Connection {
                reader: Some(reader),
                writer: Some(writer),
            };
            ctx.add_stream(Compat::new(
                act.stem_connection
                    .reader
                    .take()
                    .unwrap()
                    .compat()
                    .map_ok(move |res| FromStem {
                        inner: res,
                        model_version: cloned_model_version.clone(),
                    }),
            ));
            ctx.address().do_send(StemConnectionSuccessful);
        })
        .map_err(move |err, _act, ctx| {
            error!("failed to connect to stem: {:?}", err);
            ctx.address().do_send(ConnectToStem {
                model_version: model_version.clone(),
            });
        })
        .wait(ctx);
}

pub fn connect_to_stem(url: String, auth_header: header::HeaderValue) -> ws::ClientHandshake {
    let mut client = ws::Client::new(url);
    client = client.header(header::AUTHORIZATION, auth_header);
    client.connect()
}

pub fn connect_to_caging_wrapper(
    url: String,
    proxy: &mut WsProxy,
    ctx: &mut ws::WebsocketContext<WsProxy>,
) {
    debug!("attempting to connect to url: {}", url.clone());

    connect_to_caging(url.clone())
        .into_actor(proxy)
        .map(move |(reader, writer), act, ctx| {
            act.caging_connection.reader = Some(reader);
            act.caging_connection.writer = Some(writer);

            ctx.add_stream(Compat::new(
                act.caging_connection
                    .reader
                    .take()
                    .unwrap()
                    .compat()
                    .map_ok(FromCaging),
            ));
        })
        .map_err(|err, _act, _ctx| {
            error!("failed to connect to caging: {:?}", err);
        })
        .wait(ctx);
}

pub fn connect_to_caging(url: String) -> ws::ClientHandshake {
    let mut client = ws::Client::new(url);
    client.connect()
}

#[derive(StructOpt, Clone)]
#[structopt(name = "config")]
struct Config {
    #[structopt(short = "v", parse(from_occurrences))]
    /// Increase the verbosity.
    verbosity: usize,
}

fn main() {
    openssl_probe::init_ssl_cert_env_vars();

    // Parse command-line arguments.
    let args = Config::from_args();

    logging::from_verbosity(
        args.verbosity,
        Some(vec![
            "tokio_threadpool",
            "tokio_reactor",
            "tokio_core",
            "mio",
            "hyper",
            "trust-dns-proto",
        ]),
    );

    let proxy_url = {
        match std::env::var("PROXY_URL") {
            Ok(url) => Some(url),
            _ => None,
        }
    };

    if proxy_url.is_none() {
        error!("failed to retreive PROXY_URL");
        std::process::exit(1);
    }

    let stem_url = {
        match std::env::var("STEM_URL") {
            Ok(url) => Some(url),
            _ => None,
        }
    };

    if stem_url.is_none() {
        error!("failed to retreive STEM_URL");
        std::process::exit(1);
    }

    let caging_url = {
        match std::env::var("CAGING_URL") {
            Ok(url) => Some(url),
            _ => None,
        }
    };

    if caging_url.is_none() {
        error!("failed to retreive CAGING_URL");
        std::process::exit(1);
    }

    server::new(move || {
        let stem_url = stem_url.clone().unwrap();
        let caging_url = caging_url.clone().unwrap();

        App::new().resource("/", move |r| {
            r.with(move |req| ws_index(req, stem_url.clone(), caging_url.clone()))
        })
    })
    .bind(proxy_url.unwrap())
    .unwrap()
    .run()
}

// TODO: separate into own module
// TODO: add back in somehow the optional fields
#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct StreamingResponse {
    pub channel_index: (u16, u16),
    pub duration: f32,
    pub start: f32,
    pub is_final: bool,
    pub channel: Channel,
}

#[derive(Deserialize, Serialize, Clone, Default, PartialEq, Debug)]
pub struct Channel {
    pub alternatives: Vec<Alternative>,
}

#[derive(Deserialize, Serialize, Clone, Default, PartialEq, Debug)]
pub struct Alternative {
    pub transcript: String,
    pub confidence: f32,
    pub words: Vec<Word>,
}

#[derive(Deserialize, Serialize, Clone, Default, PartialEq, Debug)]
pub struct Word {
    pub word: String,
    pub start: f32,
    pub end: f32,
    pub confidence: f32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<Vec<u8>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub speaker: Option<i64>,
    #[serde(skip)]
    pub _full_vec: Option<Vec<f32>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub full_vec: Option<HashMap<String, f32>>,
}
