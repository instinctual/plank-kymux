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
        roots: Option<rustls::RootCertStore>,
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
            roots,
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
                    roots,
                    &options,
                    credentials,
                )
                .await?
            } else {
                kyproto::Connection::quinn_connect(addr, &server_name, roots, &options).await?
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
