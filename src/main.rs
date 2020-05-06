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

fn ws_index(req: HttpRequest, stem_url: String) -> Result<HttpResponse, ActixError> {
    let ws_proxy = WsProxy::with_request(req.clone(), stem_url.clone())?;
    ws::start(&req, ws_proxy)
}

pub struct WsProxy {
    stem_url: String,
    query_string: QueryString,
    auth_header: header::HeaderValue,
    buffer: Vec<ws::Message>,
    stem_connections: HashMap<String, Connection>,
    current_model: String,
    caging_connection: Connection,
    current_vocab: String,
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
    pub fn with_request(req: HttpRequest, stem_url: String) -> Result<Self, ActixError> {
        match req.headers().get(header::AUTHORIZATION) {
            Some(auth_header) => {
                let query_string = QueryString::from_str(req.query_string())?;

                let mut stem_connections = HashMap::new();

                // TODO: the following needs to be based on the model requested by the client, and its associated models
                stem_connections.insert("general".to_string(), Default::default());
                stem_connections.insert("phonecall".to_string(), Default::default());
                stem_connections.insert("meeting".to_string(), Default::default());
                let current_model = "general".to_string();

                // TODO: need way to switch vocab
                let caging_connection = Default::default();
                let current_vocab = "aurora".to_string();

                Ok(Self {
                    stem_url,
                    query_string,
                    auth_header: auth_header.clone(),
                    buffer: Vec::new(),
                    stem_connections,
                    current_model,
                    caging_connection,
                    current_vocab,
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
        for model in self.stem_connections.keys() {
            keys.push(model.clone());
        }
        for model in keys {
            connect_to_stem_wrapper(
                self.stem_url.clone(),
                self.query_string.clone(),
                model.clone().to_string(),
                self.auth_header.clone(),
                self,
                ctx,
            );
        }

        // open connection to the caging server
        connect_to_caging_wrapper("ws://localhost:8765".to_string(), self, ctx);
    }

    fn stopping(&mut self, _ctx: &mut Self::Context) -> Running {
        info!("WsProxy Actor is stopping");

        for (model, mut stem_connection) in self.stem_connections.drain() {
            let writer = stem_connection.writer.borrow_mut();
            if let Some(writer) = writer.as_mut() {
                info!("closing writer for model: {}", model);
                writer.close(None);
            }
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

        if let Some(writer) = self
            .stem_connections
            .get_mut(&self.current_model)
            .unwrap()
            .writer
            .as_mut()
        {
            // TODO: check unwrap
            for msg in self.buffer.drain(..) {
                match msg {
                    ws::Message::Text(text) => {
                        // TODO: make this a model or a vocab change, with only a couple of options
                        info!(
                            "text from client, will interpret as a model change: {}",
                            text.clone()
                        );
                        self.current_model = text;
                    }
                    ws::Message::Binary(binary) => {
                        trace!(
                            "binary from client, to stem with length: {}",
                            binary.clone().len()
                        );
                        writer.binary(binary.clone());
                    }
                    ws::Message::Close(reason) => {
                        info!("close from client with reason: {:?}", reason);
                        ctx.stop();
                    }
                    _ => (),
                }
            }
        }

        if self
            .stem_connections
            .get_mut(&self.current_model)
            .unwrap()
            .writer
            .is_none()
        {
            // TODO: check unwrap
            warn!("proxy is not ready yet, buffering message from client, to stem");
            // TODO: don't try to reconnect if I'm already trying to reconnect ?
            connect_to_stem_wrapper(
                self.stem_url.clone(),
                self.query_string.clone(),
                self.current_model.clone(),
                self.auth_header.clone(),
                self,
                ctx,
            );
        }
    }
}

struct FromStem(ws::Message);

impl FromStem {
    pub fn into_inner(self) -> ws::Message {
        self.0
    }
}

impl Message for FromStem {
    type Result = ();
}

impl StreamHandler<FromStem, ws::ProtocolError> for WsProxy {
    fn handle(&mut self, msg: FromStem, _ctx: &mut Self::Context) {
        if let Some(writer) = self.caging_connection.writer.as_mut() {
            match msg.into_inner() {
                ws::Message::Text(text) => {
                    trace!("text from stem, to caging: {}", text);
                    let streaming_response: StreamingResponse =
                        serde_json::from_str(&text).unwrap(); // TODO: check unwrap
                    let to_caging = serde_json::to_string(&ToCaging {
                        streaming_response,
                        vocab: self.current_vocab.clone(),
                    })
                    .unwrap(); // TODO: check unwrap
                    writer.text(to_caging);
                }
                ws::Message::Binary(binary) => {
                    debug!(
                        "binary from stem, for some reason with length: {}",
                        binary.len()
                    );
                }
                _ => {}
            }
        } else {
            error!("connection to caging server not up");
        }
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
}

pub fn connect_to_stem_wrapper(
    url: String,
    query_string: QueryString,
    model: String,
    auth_header: header::HeaderValue,
    proxy: &mut WsProxy,
    ctx: &mut ws::WebsocketContext<WsProxy>,
) {
    let mut query_string = query_string;
    query_string.push("model", &model);
    let query_string = query_string.to_str().unwrap_or_default();
    let url = format!("{}?{}", url, query_string);

    info!("attempting to connect to url: {}", url.clone());

    connect_to_stem(url.clone(), auth_header.clone())
        .into_actor(proxy)
        .map(move |(reader, writer), act, ctx| {
            act.stem_connections.insert(
                model.clone(),
                Connection {
                    reader: Some(reader),
                    writer: Some(writer),
                },
            );
            ctx.add_stream(Compat::new(
                act.stem_connections
                    .get_mut(&model.clone())
                    .unwrap() // TODO: check this unwrap
                    .reader
                    .take()
                    .unwrap()
                    .compat()
                    .map_ok(FromStem),
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
            error!("failed to connect to stem: {:?}", err);
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

    // TODO: add caging URL
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

    server::new(move || {
        let stem_url = stem_url.clone().unwrap();

        App::new().resource("/", move |r| {
            r.with(move |req| ws_index(req, stem_url.clone()))
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
