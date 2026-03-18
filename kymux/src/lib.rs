// Project Kyber: lib.rs
// Copyright © 2022-2026 Kyber SAS
// SPDX-License-Identifier: LicenseRef-Kyber-Commercial OR AGPL-3.0
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

mod error;

#[cfg(feature = "ipc")]
pub mod ipc;

use std::time::Duration;

pub use error::{Error, Result};
#[cfg(feature = "ipc")]
pub use kycom;
pub use kymux_types as types;
pub use kynet;
pub use kyproto;

#[allow(dead_code)]
const KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(5);

#[cfg(feature = "server")]
pub struct ServerConfig {
    pub addr: std::net::SocketAddr,
    pub cert_chain: Vec<rustls::pki_types::CertificateDer<'static>>,
    pub private_key: rustls::pki_types::PrivateKeyDer<'static>,
}

pub struct WebTransportCertificateHash {
    pub hash_algorithm: String,
    pub hash: String,
}

pub enum ClientConfig {
    #[cfg(feature = "backend-quinn")]
    Quic {
        addr: std::net::SocketAddr,
        tls_config: Option<rustls::ClientConfig>,
        server_name: String,
        /// SHA-256 hash of the expected server certificate (hex encoded).
        /// If provided, the server certificate will be validated by comparing its hash
        /// instead of using CA chain validation.
        certificate_hash: Option<String>,
    },
    #[cfg(feature = "backend-webtransport-js")]
    WebTransport {
        url: String,
        certificate_hash: Option<WebTransportCertificateHash>,
    },
}

// Accept a single connection
#[cfg(feature = "server")]
pub struct Server {
    inner: Box<dyn kyproto::Server>,
}

#[cfg(feature = "server")]
impl Server {
    pub async fn new(config: ServerConfig) -> Result<Self> {
        let server = kyproto::Connection::common_start_server_on_addr(
            config.addr,
            config.cert_chain,
            config.private_key,
            &kyproto::common::CommonServerOptions {
                keep_alive_interval: Some(KEEP_ALIVE_INTERVAL),
                ..Default::default()
            },
        )?;

        Ok(Self {
            inner: Box::new(server),
        })
    }

    pub async fn accept(&self) -> Result<kyproto::Connection> {
        let connection = self.inner.accept().await?;
        Ok(connection)
    }

    pub async fn accept_with_auth(&self) -> Result<kyproto::UnauthenticatedConnection> {
        let connection = self.inner.accept_with_auth().await?;
        Ok(connection)
    }

    pub fn close(&self, error_code: u32, reason: &str) {
        self.inner.close(error_code, reason)
    }

    pub async fn wait_idle(&self) {
        self.inner.wait_idle().await
    }
}

async fn connect_internal(
    config: ClientConfig,
    credentials: Option<&kyproto::ClientAuth>,
) -> Result<kyproto::Connection> {
    match config {
        #[cfg(feature = "backend-quinn")]
        ClientConfig::Quic {
            addr,
            tls_config,
            server_name,
            certificate_hash,
        } => {
            let options = kyproto::quinn::QuinnClientOptions {
                keep_alive_interval: Some(KEEP_ALIVE_INTERVAL),
                certificate_hash,
                ..Default::default()
            };

            let kyproto = if let Some(credentials) = credentials {
                kyproto::Connection::quinn_connect_with_auth(
                    addr,
                    &server_name,
                    tls_config,
                    &options,
                    credentials,
                )
                .await?
            } else {
                kyproto::Connection::quinn_connect(addr, &server_name, tls_config, &options).await?
            };

            Ok(kyproto)
        }
        #[cfg(feature = "backend-webtransport-js")]
        ClientConfig::WebTransport {
            url,
            certificate_hash,
        } => {
            use kyproto::webtransport_js::{
                WebTransportJSCongestionControl, WebTransportJSHash, WebTransportJSOptions,
            };

            let server_certificate_hashes = if let Some(certificate_hash) = certificate_hash {
                Some(vec![WebTransportJSHash::new_from_hex(
                    certificate_hash.hash_algorithm,
                    &certificate_hash.hash,
                )?])
            } else {
                None
            };

            let options = WebTransportJSOptions {
                congestion_control: WebTransportJSCongestionControl::LowLatency,
                require_unreliable: true,
                server_certificate_hashes,
            };

            let kyproto = if let Some(credentials) = credentials {
                kyproto::Connection::webtransport_js_connect_with_auth(&url, &options, credentials)
                    .await?
            } else {
                kyproto::Connection::webtransport_js_connect(&url, &options).await?
            };

            Ok(kyproto)
        }
    }
}

pub async fn connect(config: ClientConfig) -> Result<kyproto::Connection> {
    connect_internal(config, None).await
}

pub async fn connect_with_auth(
    config: ClientConfig,
    credentials: &kyproto::ClientAuth,
) -> Result<kyproto::Connection> {
    connect_internal(config, Some(credentials)).await
}
