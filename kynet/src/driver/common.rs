// Project Kyber: common.rs
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

use crate::Connection;
use crate::cert::{Certificate, PrivateKey};
use crate::error::ConnectionError;

use std::net::{Ipv6Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use socket2::{Domain, Protocol, Socket, Type};

use super::quinn::KYMUX_ALPN;

#[derive(Default)]
pub struct CommonServerOptions {
    pub max_idle_timeout: Option<Duration>,
    pub keep_alive_interval: Option<Duration>,
}

/// Common server that accepts both QUIC (Kymux) and WebTransport connections
/// on the same port, dispatching based on negotiated ALPN.
pub struct CommonServer {
    quinn_endpoint: Arc<quinn::Endpoint>,
}

impl CommonServer {
    pub fn start_on_addr(
        addr: SocketAddr,
        cert_chain: Vec<Certificate>,
        key: PrivateKey,
        options: &CommonServerOptions,
    ) -> Result<Self, ConnectionError> {
        // Create rustls config with ALPN support for both protocols
        let mut crypto_config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(cert_chain, key)
            .map_err(|e| ConnectionError(format!("TLS config error: {:?}", e)))?;

        // Configure ALPN based on enabled features
        #[allow(unused_mut)]
        let mut alpn_protocols = vec![KYMUX_ALPN.to_vec()];
        #[cfg(feature = "kynet-wtransport")]
        alpn_protocols.push(wtransport_proto::WEBTRANSPORT_ALPN.to_vec());
        crypto_config.alpn_protocols = alpn_protocols;

        // Create Quinn server config
        let mut server_config = quinn::ServerConfig::with_crypto(Arc::new(
            quinn::crypto::rustls::QuicServerConfig::try_from(crypto_config)
                .map_err(|e| ConnectionError(format!("QUIC config error: {:?}", e)))?,
        ));

        // Configure transport
        let mut transport_config = quinn::TransportConfig::default();
        transport_config.max_idle_timeout(
            options
                .max_idle_timeout
                .map(quinn::IdleTimeout::try_from)
                .transpose()
                .map_err(|_| ConnectionError("Invalid max_idle_timeout".to_string()))?,
        );
        transport_config.keep_alive_interval(options.keep_alive_interval);
        server_config.transport_config(Arc::new(transport_config));

        // Create UDP socket with dual-stack support when binding to IPv6.
        // quinn::Endpoint::server() doesn't set IPV6_V6ONLY=false, which means
        // on Windows an IPv6 socket won't accept IPv4 connections. We replicate
        // the approach used by quinn's client() method (see quinn endpoint.rs).
        let domain = Domain::for_address(addr);
        let socket = Socket::new(domain, Type::DGRAM, Some(Protocol::UDP))
            .map_err(|e| ConnectionError(format!("Socket creation failed: {e:?}")))?;

        if addr.is_ipv6()
            && let Err(e) = socket.set_only_v6(false)
        {
            log::debug!("Unable to make socket dual-stack: {e}");
        }

        socket
            .bind(&addr.into())
            .map_err(|e| ConnectionError(format!("Socket bind failed: {e:?}")))?;

        let udp_socket: std::net::UdpSocket = socket.into();
        let runtime = Arc::new(quinn::TokioRuntime);

        let quinn_endpoint = quinn::Endpoint::new(
            quinn::EndpointConfig::default(),
            Some(server_config),
            udp_socket,
            runtime,
        )
        .map_err(|e| ConnectionError(format!("Failed to create endpoint: {:?}", e)))?;

        Ok(Self {
            quinn_endpoint: Arc::new(quinn_endpoint),
        })
    }

    pub fn start(
        port: u16,
        cert_chain: Vec<Certificate>,
        key: PrivateKey,
        options: &CommonServerOptions,
    ) -> Result<Self, ConnectionError> {
        let addr = SocketAddr::new(Ipv6Addr::UNSPECIFIED.into(), port);
        Self::start_on_addr(addr, cert_chain, key, options)
    }
}

#[async_trait]
impl super::Server for CommonServer {
    async fn accept(&self) -> Result<Option<Connection>, ConnectionError> {
        let Some(quinn_incoming) = self.quinn_endpoint.accept().await else {
            return Ok(None);
        };

        #[allow(unused_mut)]
        let mut quinn_connecting = quinn_incoming
            .accept()
            .map_err(|e| ConnectionError(format!("Failed to accept connection: {:?}", e)))?;

        // If WebTransport is enabled, check ALPN to dispatch
        #[cfg(feature = "kynet-wtransport")]
        {
            let alpn = quinn_connecting
                .handshake_data()
                .await
                .ok()
                .and_then(|data| {
                    data.downcast::<quinn::crypto::rustls::HandshakeData>()
                        .ok()
                        .and_then(|h| h.protocol.clone())
                });

            if alpn.as_deref() == Some(wtransport_proto::WEBTRANSPORT_ALPN) {
                // WebTransport connection (ALPN: "h3")
                let session_request =
                    wtransport::endpoint::IncomingSessionFuture::with_quic_connecting(
                        quinn_connecting,
                    )
                    .await
                    .map_err(|e| ConnectionError(format!("WebTransport accept failed: {:?}", e)))?;

                let wt_conn = session_request.accept().await.map_err(|e| {
                    ConnectionError(format!("WebTransport session accept failed: {:?}", e))
                })?;

                return Ok(Some(Connection::from(wt_conn)));
            }
        }

        // Kymux over QUIC connection (ALPN: "kymux" or no WebTransport support)
        let quinn_connection = quinn_connecting
            .await
            .map_err(|e| ConnectionError(format!("Connection failed: {:?}", e)))?;
        Ok(Some(quinn_connection.into()))
    }

    fn close(&self, error_code: u32, reason: &str) {
        self.quinn_endpoint
            .close(error_code.into(), reason.as_bytes());
    }

    async fn wait_idle(&self) {
        self.quinn_endpoint.wait_idle().await;
    }
}
