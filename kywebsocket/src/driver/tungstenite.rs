// Project Kyber: tungstenite.rs
// Copyright © 2022-2026 Kyber SAS
// SPDX-License-Identifier: LicenseRef-Kyber-Commercial OR AGPL-3.0-or-later
//
// This file is both under dual license: AGPLv3 and a Commercial one.
//
// ----
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use super::{MessageRecvStreamDriver, MessageSendStreamDriver};
use crate::{Connection, MessageRecvStream, MessageSendStream};

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::{SinkExt, StreamExt, TryStreamExt};
use rustls_platform_verifier::ConfigVerifierExt;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
pub use tokio_tungstenite::tungstenite::http::{HeaderMap, HeaderName, HeaderValue};

pub use tokio_tungstenite::tungstenite::http::header::COOKIE;

#[derive(Debug)]
pub enum TlsConfig {
    ServerHash(Vec<u8>),
    Platform,
    Custom(rustls::ClientConfig),
}

pub async fn connect(
    url: &str,
    explicit_addr: Option<SocketAddr>,
    tls_config: TlsConfig,
    headers: HeaderMap,
) -> io::Result<Connection> {
    let mut request = url.into_client_request().map_err(tungstenite_to_io_error)?;

    request.headers_mut().extend(headers);

    let uri = request.uri();
    let host = uri
        .host()
        .ok_or_else(|| io::Error::other("URL has no host"))?
        // http::Uri keeps the brackets on IPv6 literals
        .trim_start_matches('[')
        .trim_end_matches(']');

    let port = uri
        .port_u16()
        .map(Ok)
        .unwrap_or_else(|| match uri.scheme_str() {
            Some("wss") => Ok(443),
            Some("ws") => Ok(80),
            other => Err(io::Error::other(format!("Unsupported scheme: {other:?}"))),
        })?;

    let tls_config = match tls_config {
        TlsConfig::ServerHash(hash) => {
            let verifier = kymux_util::cert::HashCertVerifier::new(hash);
            rustls::ClientConfig::builder()
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(verifier))
                .with_no_client_auth()
        }
        TlsConfig::Platform => rustls::ClientConfig::with_platform_verifier()
            .map_err(|e| io::Error::other(format!("Platform verifier error: {e}")))?,
        TlsConfig::Custom(tls_config) => tls_config,
    };

    let connector = tokio_rustls::TlsConnector::from(Arc::new(tls_config));
    let dnsname = rustls_pki_types::ServerName::try_from(host.to_owned())
        .map_err(|e| io::Error::other(format!("Invalid server name: {e}")))?;

    let stream = if let Some(addr) = explicit_addr {
        TcpStream::connect(addr).await?
    } else {
        TcpStream::connect((host, port)).await?
    };

    let stream = connector.connect(dnsname, stream).await?;

    let (stream, _response) = tokio_tungstenite::client_async(request, stream)
        .await
        .map_err(tungstenite_to_io_error)?;
    let (send, recv) = stream.split();

    let send = send.sink_map_err(tungstenite_to_io_error);
    let send = TungsteniteMessageSendStreamDriver::new(Box::new(send));
    let send = MessageSendStream::new(send);

    let recv = recv.map_err(tungstenite_to_io_error);
    let recv = TungsteniteMessageRecvStreamDriver::new(Box::new(recv));
    let recv = MessageRecvStream::new(recv);

    Ok(Connection { send, recv })
}

struct TungsteniteMessageSendStreamDriver<T> {
    send: T,
}

// tokio_tungstenite::WebSocketSender does not implement Debug
impl<T> std::fmt::Debug for TungsteniteMessageSendStreamDriver<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("TungsteniteMessageSendStreamDriver")
    }
}

impl<T> TungsteniteMessageSendStreamDriver<T> {
    fn new(send: T) -> Self {
        Self { send }
    }
}

#[async_trait]
impl<T> MessageSendStreamDriver for TungsteniteMessageSendStreamDriver<T>
where
    T: futures::Sink<tungstenite::protocol::Message, Error = io::Error> + Send + Sync + Unpin,
{
    async fn send(&mut self, msg: Bytes) -> io::Result<()> {
        let msg = tungstenite::protocol::Message::Binary(msg);
        self.send.send(msg).await
    }

    async fn close(mut self: Box<Self>) {
        let _ = self.send.close().await;
    }
}

struct TungsteniteMessageRecvStreamDriver<T> {
    recv: T,
}

// tokio_tungstenite::WebSocketReceiver does not implement Debug
impl<T> std::fmt::Debug for TungsteniteMessageRecvStreamDriver<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("TungsteniteMessageRecvStreamDriver")
    }
}

impl<T> TungsteniteMessageRecvStreamDriver<T> {
    fn new(recv: T) -> Self {
        Self { recv }
    }
}

#[async_trait]
impl<T> MessageRecvStreamDriver for TungsteniteMessageRecvStreamDriver<T>
where
    T: futures::Stream<Item = io::Result<tungstenite::protocol::Message>> + Send + Sync + Unpin,
{
    async fn recv(&mut self) -> io::Result<Option<Bytes>> {
        let msg = self.recv.next().await.transpose()?;
        match msg {
            Some(tungstenite::protocol::Message::Binary(bytes)) => Ok(Some(bytes)),
            Some(tungstenite::protocol::Message::Close(None)) => {
                log::info!("Websocket closed (no reason given)");
                Ok(None)
            }
            Some(tungstenite::protocol::Message::Close(Some(close_reason))) => {
                log::info!(
                    "Websocket closed: {:?} {:?}",
                    close_reason.code,
                    close_reason.reason
                );
                Ok(None)
            }
            None => Ok(None),
            _ => {
                log::warn!("Unexpected tungstenite message: {msg:?}");
                Err(io::Error::other(
                    "Unexpected tungstenite message type".to_string(),
                ))
            }
        }
    }
}

fn tungstenite_to_io_error(err: tungstenite::error::Error) -> io::Error {
    match err {
        tungstenite::error::Error::Io(err) => err,
        err => io::Error::other(err),
    }
}
