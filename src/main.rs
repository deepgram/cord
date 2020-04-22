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
use std::str::FromStr;

fn ws_index(req: HttpRequest, destination_url: String) -> Result<HttpResponse, ActixError> {
    let ws_proxy = WsProxy::with_request(req.clone(), destination_url.clone())?;
    ws::start(&req, ws_proxy)
}

pub struct WsProxy {
    destination_url: String,
    query_string: QueryString,
    auth_header: header::HeaderValue,
    reader: Option<ws::ClientReader>,
    writer: Option<ws::ClientWriter>,
    buffer: Vec<ws::Message>,
}

impl WsProxy {
    pub fn with_request(req: HttpRequest, destination_url: String) -> Result<Self, ActixError> {
        match req.headers().get(header::AUTHORIZATION) {
            Some(auth_header) => Ok(Self {
                destination_url,
                query_string: QueryString::from_str(req.query_string())?,
                auth_header: auth_header.clone(),
                reader: None,
                writer: None,
                buffer: Vec::new(),
            }),
            None => {
                error!("invalid or missing credentials");
                Err(ErrorUnauthorized("Invalid or missing credentials."))
            }
        }
    }
}

impl Actor for WsProxy {
    type Context = ws::WebsocketContext<Self>;

    fn stopping(&mut self, _ctx: &mut Self::Context) -> Running {
        info!("WsProxy Actor is stopping");

        Running::Stop
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        info!("WsProxy Actor has stopped");
    }
}

impl StreamHandler<ws::Message, ws::ProtocolError> for WsProxy {
    fn handle(&mut self, msg: ws::Message, ctx: &mut Self::Context) {
        self.buffer.push(msg);

        if let Some(writer) = self.writer.as_mut() {
            for msg in self.buffer.drain(..) {
                match msg {
                    ws::Message::Text(text) => {
                        trace!("text from client, to destination: {}", text.clone());
                        writer.text(text.clone());
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

        if self.writer.is_none() {
            warn!("proxy is not ready yet, buffering message from client, to destination");
            connect_to_destination_wrapper(
                self.destination_url.clone(),
                self.query_string.clone(),
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
    auth_header: header::HeaderValue,
    proxy: &mut WsProxy,
    ctx: &mut ws::WebsocketContext<WsProxy>,
) {
    let query_string = query_string.to_str().unwrap_or_default();
    let url = format!("{}?{}", url, query_string);

    trace!("attempting to connect to url: {}", url.clone());

    connect_to_destination(url.clone(), auth_header.clone())
        .into_actor(proxy)
        .map(|(reader, writer), act, ctx| {
            act.reader = Some(reader);
            act.writer = Some(writer);
            ctx.add_stream(Compat::new(
                act.reader.take().unwrap().compat().map_ok(FromDestination),
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
