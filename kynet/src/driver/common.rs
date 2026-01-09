use crate::cert::{Certificate, PrivateKey};
use crate::error::ConnectionError;
use crate::Connection;

use std::net::{Ipv6Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

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

        // Create Quinn endpoint
        let quinn_endpoint = quinn::Endpoint::server(server_config, addr)
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
    async fn accept(&self) -> Result<Connection, ConnectionError> {
        let connecting = self
            .quinn_endpoint
            .accept()
            .await
            .ok_or_else(|| ConnectionError("Endpoint closed".to_string()))?;

        let quinn_conn = connecting
            .await
            .map_err(|e| ConnectionError(format!("Connection failed: {:?}", e)))?;

        // If WebTransport is enabled, check ALPN to dispatch
        #[cfg(feature = "kynet-wtransport")]
        {
            let alpn = quinn_conn.handshake_data().and_then(|data| {
                data.downcast::<quinn::crypto::rustls::HandshakeData>()
                    .ok()
                    .and_then(|h| h.protocol.clone())
            });

            if alpn.as_deref() == Some(wtransport_proto::WEBTRANSPORT_ALPN) {
                // WebTransport connection (ALPN: "h3")
                let session_request =
                    wtransport::endpoint::IncomingSessionFuture::accept_from_connection(quinn_conn)
                        .await
                        .map_err(|e| {
                            ConnectionError(format!("WebTransport accept failed: {:?}", e))
                        })?;

                let wt_conn = session_request.accept().await.map_err(|e| {
                    ConnectionError(format!("WebTransport session accept failed: {:?}", e))
                })?;

                return Ok(Connection::from(wt_conn));
            }
        }

        // Kymux over QUIC connection (ALPN: "kymux" or no WebTransport support)
        Ok(quinn_conn.into())
    }

    fn close(&self, error_code: u32, reason: &str) {
        self.quinn_endpoint
            .close(error_code.into(), reason.as_bytes());
    }

    async fn wait_idle(&self) {
        self.quinn_endpoint.wait_idle().await;
    }
}
