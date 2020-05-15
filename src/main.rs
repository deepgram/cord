use actix::prelude::*;
use actix_web::error::{Error as ActixError, ErrorUnauthorized};
use actix_web::http::header;
use actix_web::ws;
use actix_web::{server, App, HttpRequest, HttpResponse};
use futures::{compat::*, prelude::*};
use structopt::StructOpt;

#[macro_use]
extern crate log;
extern crate chrono;
extern crate env_logger;
mod logging;

mod query_string;
use query_string::QueryString;
use std::borrow::BorrowMut;
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
    buffer: Vec<ws::Message>,
    stem_connections: HashMap<ModelVersion, Connection>,
    model_version: ModelVersion,
    pow_model_version: ModelVersion,
    current_model_version: ModelVersion,
    caging_connection: Connection,
    current_vocab: Option<String>,
    switch_lock: bool,
    temp_duration: HashMap<ModelVersion, f32>,
    offset_duration: f32,
    stop: bool,
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

                let mut stem_connections = HashMap::new();

                stem_connections.insert(model_version.clone(), Default::default());
                stem_connections.insert(pow_model_version.clone(), Default::default());
                let current_model_version = model_version.clone();

                let caging_connection = Default::default();
                let current_vocab = None;

                let mut temp_duration = HashMap::new();
                temp_duration.insert(model_version.clone(), 0.0);
                temp_duration.insert(pow_model_version.clone(), 0.0);

                Ok(Self {
                    stem_url,
                    caging_url,
                    query_string,
                    auth_header: auth_header.clone(),
                    buffer: Vec::new(),
                    stem_connections,
                    model_version,
                    pow_model_version,
                    current_model_version,
                    caging_connection,
                    current_vocab,
                    switch_lock: false,
                    temp_duration,
                    offset_duration: 0.0,
                    stop: false,
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
        info!("WsProxy Actor has started");

        // open connections to stem for each model
        let mut keys = Vec::new();
        for model_version in self.stem_connections.keys() {
            keys.push(model_version.clone());
        }
        for model_version in keys {
            connect_to_stem_wrapper(
                self.stem_url.clone(),
                self.query_string.clone(),
                model_version.clone(),
                self.auth_header.clone(),
                self,
                ctx,
            );
        }

        // open connection to the caging server
        connect_to_caging_wrapper(self.caging_url.clone(), self, ctx);
    }

    fn stopping(&mut self, _ctx: &mut Self::Context) -> Running {
        info!("WsProxy Actor is stopping");

        if !self.stop {
            info!("WsProxy Actor will continue, since we haven't gotten a request from the client to close the connection");
            return Running::Continue;
        }

        info!("WsProxy Actor will stop, since we did get a request from the client to close the connection");

        if !self.buffer.is_empty() {
            warn!(
                "buffer not empty, will miss the last {} messages!",
                self.buffer.len()
            );
        }

        for (model_version, mut stem_connection) in self.stem_connections.drain() {
            let writer = stem_connection.writer.borrow_mut();
            if let Some(writer) = writer.as_mut() {
                info!("closing writer for model_version: {:?}", model_version);
                writer.close(None);
            }
        }
        if let Some(writer) = self.caging_connection.writer.as_mut() {
            info!("closing writer caging server");
            writer.close(None);
        }

        Running::Stop
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        info!("WsProxy Actor has stopped");
    }
}

impl StreamHandler<ws::Message, ws::ProtocolError> for WsProxy {
    fn handle(&mut self, msg: ws::Message, ctx: &mut Self::Context) {
        self.buffer.push(msg);

        if self
            .stem_connections
            .get_mut(&self.current_model_version)
            .unwrap()
            .writer
            .is_none()
        {
            // TODO: check unwrap
            warn!(
                "proxy for {:?} is not ready yet, buffering message from client, to stem",
                self.current_model_version
            );
            // TODO: don't try to reconnect if I'm already trying to reconnect ?
            if !self.switch_lock {
                info!(
                    "switch_lock is false, so the proxy should be (re)connected for {:?}",
                    self.current_model_version
                );
                connect_to_stem_wrapper(
                    self.stem_url.clone(),
                    self.query_string.clone(),
                    self.current_model_version.clone(),
                    self.auth_header.clone(),
                    self,
                    ctx,
                );
            } else {
                info!(
                    "switch_lock is true, so the connection to stem for {:?} will be deferred",
                    self.current_model_version
                );
            }
        }

        let mut kill_writer = false;

        if let Some(writer) = self
            .stem_connections
            .get_mut(&self.current_model_version)
            .unwrap()
            .writer
            .as_mut()
        {
            // TODO: check unwrap
            for msg in self.buffer.drain(..) {
                match msg {
                    ws::Message::Text(text) => {
                        if !self.switch_lock {
                            info!(
                                "text from client, will interpret as a vocab/model change: {}",
                                text.clone()
                            );

                            let empty: Vec<u8> = Vec::new();
                            writer.binary(empty); // attempt to close the connection gracefully ...

                            kill_writer = true;
                            self.switch_lock = true;

                            if text == "" {
                                self.current_vocab = None;
                                self.current_model_version = self.model_version.clone();
                            } else {
                                self.current_vocab = Some(text);
                                self.current_model_version = self.pow_model_version.clone();
                            }
                        } else {
                            warn!("client attempted to switch vocab/model, but switch_lock is true, so ignoring this request");
                        }
                    }
                    ws::Message::Binary(binary) => {
                        trace!(
                            "binary from client, to stem with length: {}",
                            binary.clone().len()
                        );

                        // it the message was empty, interpret it as a stop command from the client
                        // however, still proxy the message so that connections to stem get closed
                        if binary.is_empty() {
                            self.stop = true;
                        }

                        writer.binary(binary.clone());
                    }
                    ws::Message::Close(reason) => {
                        info!("close from client with reason: {:?}", reason);
                        ctx.close(reason.clone());
                        ctx.stop();
                    }
                    _ => (),
                }
            }
        }

        // setting the writer to none here is ok because when this stream gets its last message
        // (and subsequent close message), the stream will re-open itself
        if kill_writer {
            self.stem_connections
                .get_mut(&self.current_model_version)
                .unwrap()
                .writer = None;
        }
    }

    fn error(&mut self, err: ws::ProtocolError, _ctx: &mut Self::Context) -> Running {
        error!("client stream got the error {:?} .", err); // TODO: should I do ctx.close here?
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
                    match &self.current_vocab {
                        Some(current_vocab) => {
                            let streaming_response: Result<
                                StreamingResponse,
                                serde_json::error::Error,
                            > = serde_json::from_str(&text);
                            match streaming_response {
                                Ok(mut streaming_response) => {
                                    // store the duration of this response - if it is the last response before a switch, it will later be set to offset_duration
                                    *self.temp_duration.get_mut(&msg.model_version).unwrap() =
                                        streaming_response.duration;

                                    // adjust timings
                                    streaming_response.start += self.offset_duration;
                                    for alternative in &mut streaming_response.channel.alternatives
                                    {
                                        for mut word in &mut alternative.words {
                                            word.start += self.offset_duration;
                                            word.end += self.offset_duration;
                                        }
                                    }

                                    trace!("text from stem, to caging: {}", text);
                                    let to_caging = serde_json::to_string(&ToCaging {
                                        streaming_response: streaming_response.clone(),
                                        vocab: current_vocab.clone(),
                                    })
                                    .unwrap(); // TODO: check unwrap
                                    writer.text(to_caging);
                                }
                                Err(_) => {
                                    debug!("metadata text from stem, to client: {}", text);
                                    writer.text(text);
                                }
                            }
                        }
                        None => {
                            trace!("text from stem, to client (not caged): {}", text); // TODO: have the caging server accept empty vocab (this helps ensure ordering)
                            let streaming_response: Result<
                                StreamingResponse,
                                serde_json::error::Error,
                            > = serde_json::from_str(&text);
                            match streaming_response {
                                Ok(mut streaming_response) => {
                                    // store the duration of this response - if it is the last response before a switch, it will later be set to offset_duration
                                    *self.temp_duration.get_mut(&msg.model_version).unwrap() =
                                        streaming_response.duration;

                                    // adjust timings
                                    streaming_response.start += self.offset_duration;
                                    for alternative in &mut streaming_response.channel.alternatives
                                    {
                                        for mut word in &mut alternative.words {
                                            word.start += self.offset_duration;
                                            word.end += self.offset_duration;
                                        }
                                    }

                                    ctx.text(serde_json::to_string(&streaming_response).unwrap());
                                    // TODO: check unwrap
                                }
                                Err(_) => {
                                    debug!("metadata text from stem, to client: {}", text);
                                    writer.text(text);
                                }
                            }
                        }
                    }
                }
                ws::Message::Binary(binary) => {
                    warn!(
                        "binary from stem, for some reason with length: {}",
                        binary.len()
                    );
                }
                ws::Message::Close(reason) => {
                    warn!("close from stem with reason: {:?}", reason);

                    self.switch_lock = false;

                    if !self.stop {
                        // re-connect to stem
                        connect_to_stem_wrapper(
                            self.stem_url.clone(),
                            self.query_string.clone(),
                            msg.model_version.clone(),
                            self.auth_header.clone(),
                            self,
                            ctx,
                        );

                        // update this, as this is the last message in this stream
                        self.offset_duration +=
                            self.temp_duration.get(&msg.model_version.clone()).unwrap();
                    } else {
                        ctx.close(reason.clone());
                        ctx.stop();
                    }
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
                    "trace - binary from caging, to client with length: {}",
                    binary.len()
                );
                ctx.binary(binary);
            }
            ws::Message::Close(reason) => {
                info!("close from caging with reason: {:?}", reason);
                ctx.close(reason.clone());
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

pub fn connect_to_stem_wrapper(
    url: String,
    query_string: QueryString,
    model_version: ModelVersion,
    auth_header: header::HeaderValue,
    proxy: &mut WsProxy,
    ctx: &mut ws::WebsocketContext<WsProxy>,
) {
    let mut query_string = query_string;
    query_string.push("model", &model_version.model);
    query_string.push("version", &model_version.version);
    let query_string = query_string.to_str().unwrap_or_default();
    let url = format!("{}?{}", url, query_string);

    info!("attempting to connect to url: {}", url.clone());

    connect_to_stem(url.clone(), auth_header.clone())
        .into_actor(proxy)
        .map(move |(reader, writer), act, ctx| {
            act.stem_connections.insert(
                model_version.clone(),
                Connection {
                    reader: Some(reader),
                    writer: Some(writer),
                },
            );
            ctx.add_stream(Compat::new(
                act.stem_connections
                    .get_mut(&model_version.clone())
                    .unwrap() // TODO: check this unwrap
                    .reader
                    .take()
                    .unwrap()
                    .compat()
                    .map_ok(move |res| FromStem {
                        inner: res,
                        model_version: model_version.clone(),
                    }),
            ));
        })
        .map_err(|err, _act, _ctx| {
            error!("failed to connect to stem: {:?}", err);
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
    info!("attempting to connect to url: {}", url.clone());

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
