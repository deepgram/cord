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

fn ws_index(req: HttpRequest, destination_url: String) -> Result<HttpResponse, ActixError> {
    let ws_proxy = WsProxy::with_request(req.clone(), destination_url.clone())?;
    ws::start(&req, ws_proxy)
}

pub struct WsProxy {
    destination_url: String,
    query_string: QueryString,
    auth_header: header::HeaderValue,
    buffer: Vec<ws::Message>,
    connections: HashMap<String, Connection>,
    current_model: String,
}

// TODO: make this an actor, that way I can carry more info (like what model was used) - or, make stem OPTIONALLY return the model used with results
#[derive(Default)]
pub struct Connection {
    reader: Option<ws::ClientReader>,
    writer: Option<ws::ClientWriter>,
}

impl WsProxy {
    pub fn with_request(req: HttpRequest, destination_url: String) -> Result<Self, ActixError> {
        match req.headers().get(header::AUTHORIZATION) {
            Some(auth_header) => {
                let query_string = QueryString::from_str(req.query_string())?;

                let mut connections = HashMap::new();

                // TODO: the following needs to be based on the model requested by the client, and its associated models
                connections.insert("general".to_string(), Default::default());
                connections.insert("phonecall".to_string(), Default::default());
                connections.insert("meeting".to_string(), Default::default());
                let current_model = "general".to_string();

                Ok(Self {
                    destination_url,
                    query_string,
                    auth_header: auth_header.clone(),
                    buffer: Vec::new(),
                    connections,
                    current_model,
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
        for model in self.connections.keys() {
            keys.push(model.clone());
        }
        for model in keys {
            connect_to_destination_wrapper(
                self.destination_url.clone(),
                self.query_string.clone(),
                model.clone().to_string(),
                self.auth_header.clone(),
                self,
                ctx,
            );
        }
    }

    fn stopping(&mut self, _ctx: &mut Self::Context) -> Running {
        info!("WsProxy Actor is stopping");

        for (model, mut connection) in self.connections.drain() {
            let writer = connection.writer.borrow_mut();
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
            .connections
            .get_mut(&self.current_model)
            .unwrap()
            .writer
            .as_mut()
        {
            // TODO: check unwrap
            for msg in self.buffer.drain(..) {
                match msg {
                    ws::Message::Text(text) => {
                        info!(
                            "text from client, will interpret as a model change: {}",
                            text.clone()
                        );
                        self.current_model = text;
                    }
                    ws::Message::Binary(binary) => {
                        trace!(
                            "binary from client, to destination with length: {}",
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
            .connections
            .get_mut(&self.current_model)
            .unwrap()
            .writer
            .is_none()
        {
            // TODO: check unwrap
            warn!("proxy is not ready yet, buffering message from client, to destination");
            // TODO: don't try to reconnect if I'm already trying to reconnect ?
            connect_to_destination_wrapper(
                self.destination_url.clone(),
                self.query_string.clone(),
                self.current_model.clone(),
                self.auth_header.clone(),
                self,
                ctx,
            );
        }
    }
}

struct FromDestination(ws::Message);

impl FromDestination {
    pub fn into_inner(self) -> ws::Message {
        self.0
    }
}

impl Message for FromDestination {
    type Result = ();
}

impl StreamHandler<FromDestination, ws::ProtocolError> for WsProxy {
    fn handle(&mut self, msg: FromDestination, ctx: &mut Self::Context) {
        match msg.into_inner() {
            ws::Message::Text(text) => {
                // TODO: analyze this text, using caged vocab, etc
                trace!("text from destination, to client: {}", text);
                ctx.text(text);
            }
            ws::Message::Binary(binary) => {
                trace!(
                    "trace - binary from destination, to client with length: {}",
                    binary.len()
                );
                ctx.binary(binary);
            }
            ws::Message::Close(reason) => {
                info!("close from destination with reason: {:?}", reason);
                ctx.close(reason.clone());
                ctx.stop();
            }
            _ => {}
        }
    }
}

pub fn connect_to_destination_wrapper(
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

    connect_to_destination(url.clone(), auth_header.clone())
        .into_actor(proxy)
        .map(move |(reader, writer), act, ctx| {
            act.connections.insert(
                model.clone(),
                Connection {
                    reader: Some(reader),
                    writer: Some(writer),
                },
            );
            ctx.add_stream(Compat::new(
                act.connections
                    .get_mut(&model.clone())
                    .unwrap() // TODO: check this unwrap
                    .reader
                    .take()
                    .unwrap()
                    .compat()
                    .map_ok(FromDestination),
            ));
        })
        .map_err(|err, _act, _ctx| {
            error!("failed to connect to the destination: {:?}", err);
        })
        .wait(ctx);
}

pub fn connect_to_destination(
    url: String,
    auth_header: header::HeaderValue,
) -> ws::ClientHandshake {
    let mut client = ws::Client::new(url);
    client = client.header(header::AUTHORIZATION, auth_header);
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

    let destination_url = {
        match std::env::var("DESTINATION_URL") {
            Ok(url) => Some(url),
            _ => None,
        }
    };

    if destination_url.is_none() {
        error!("failed to retreive DESTINATION_URL");
        std::process::exit(1);
    }

    server::new(move || {
        let destination_url = destination_url.clone().unwrap();

        App::new().resource("/", move |r| {
            r.with(move |req| ws_index(req, destination_url.clone()))
        })
    })
    .bind(proxy_url.unwrap())
    .unwrap()
    .run()
}
