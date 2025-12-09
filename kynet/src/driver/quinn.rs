use crate::cert::{Certificate, PrivateKey, RootCertStore};
use crate::error::*;
use crate::{
    Connection, ConnectionDriver, ConnectionStats, RecvStream, RecvStreamDriver, SendStream,
    SendStreamDriver,
};

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;

/// Certificate verifier that validates server certificates by comparing their SHA-256 hash.
/// This is used for connecting to servers with self-signed certificates
/// where the expected hash is provided out-of-band (similar to WebTransport's serverCertificateHashes).
#[derive(Debug)]
struct HashCertVerifier {
    expected_hash: Vec<u8>,
}

impl HashCertVerifier {
    fn new(expected_hash_hex: &str) -> Result<Self, ConnectionError> {
        let expected_hash = hex::decode(expected_hash_hex)
            .map_err(|_| ConnectionError("Invalid certificate hash hex string".to_string()))?;
        Ok(Self { expected_hash })
    }
}

impl rustls::client::danger::ServerCertVerifier for HashCertVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        use ring::digest::{digest, SHA256};
        let actual_hash = digest(&SHA256, end_entity.as_ref());
        if actual_hash.as_ref() == self.expected_hash.as_slice() {
            Ok(rustls::client::danger::ServerCertVerified::assertion())
        } else {
            Err(rustls::Error::General(format!(
                "Certificate hash mismatch. Expected: {}, Got: {}",
                hex::encode(&self.expected_hash),
                hex::encode(actual_hash.as_ref())
            )))
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &rustls::crypto::ring::default_provider().signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &rustls::crypto::ring::default_provider().signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

impl From<quinn::Connection> for Connection {
    fn from(value: quinn::Connection) -> Self {
        Self::new(QuinnConnectionDriver::wrap(value))
    }
}

#[derive(Default)]
pub struct QuinnServerOptions {
    pub max_idle_timeout: Option<Duration>,
    pub keep_alive_interval: Option<Duration>,
}

pub struct QuinnServer {
    endpoint: quinn::Endpoint,
}

impl QuinnServer {
    pub fn start_on_addr(
        addr: SocketAddr,
        cert_chain: Vec<Certificate>,
        key: PrivateKey,
        options: &QuinnServerOptions,
    ) -> Result<Self, ConnectionError> {
        let mut config = quinn::ServerConfig::with_single_cert(cert_chain, key)?;

        let mut transport_config = quinn::TransportConfig::default();
        transport_config.max_idle_timeout(
            options
                .max_idle_timeout
                .map(quinn::IdleTimeout::try_from)
                .transpose()
                .map_err(|_| ConnectionError("Invalid max_idle_timeout".to_string()))?,
        );
        transport_config.keep_alive_interval(options.keep_alive_interval);
        config.transport_config(Arc::new(transport_config));

        let endpoint = quinn::Endpoint::server(config, addr)?;
        Ok(Self { endpoint })
    }

    pub fn start(
        port: u16,
        cert_chain: Vec<Certificate>,
        key: PrivateKey,
        options: &QuinnServerOptions,
    ) -> Result<Self, ConnectionError> {
        let addr = SocketAddr::new(Ipv4Addr::UNSPECIFIED.into(), port);
        Self::start_on_addr(addr, cert_chain, key, options)
    }
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl super::Server for QuinnServer {
    async fn accept(&self) -> Result<Connection, ConnectionError> {
        let connecting = self
            .endpoint
            .accept()
            .await
            .ok_or_else(|| ConnectionError("Endpoint closed".to_string()))?;
        let connection = connecting.await?;
        Ok(connection.into())
    }

    fn close(&self, error_code: u32, reason: &str) {
        let var_int = quinn::VarInt::from_u32(error_code);
        self.endpoint.close(var_int, reason.as_bytes());
    }

    async fn wait_idle(&self) {
        self.endpoint.wait_idle().await;
    }
}

#[derive(Default)]
pub struct QuinnClientOptions {
    pub max_idle_timeout: Option<Duration>,
    pub keep_alive_interval: Option<Duration>,
    /// SHA-256 hash of the expected server certificate (hex encoded).
    /// If provided, the server certificate will be validated by comparing its hash
    /// instead of using CA chain validation. This is similar to WebTransport's
    /// serverCertificateHashes option.
    pub certificate_hash: Option<String>,
}

#[derive(Debug)]
pub(crate) struct QuinnConnectionDriver {
    conn: quinn::Connection,
}

impl QuinnConnectionDriver {
    fn wrap(conn: quinn::Connection) -> Self {
        Self { conn }
    }

    pub async fn connect(
        addr: SocketAddr,
        server_name: &str,
        certs: Option<RootCertStore>,
        options: &QuinnClientOptions,
    ) -> Result<Connection, ConnectionError> {
        // Build rustls ClientConfig
        // Priority: certificate_hash > root_certs > error
        let tls_config = if let Some(ref hash) = options.certificate_hash {
            // Use hash-based verification (similar to WebTransport's serverCertificateHashes)
            let verifier = HashCertVerifier::new(hash)?;
            rustls::ClientConfig::builder()
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(verifier))
                .with_no_client_auth()
        } else if let Some(certs) = certs {
            // Use CA chain validation
            rustls::ClientConfig::builder()
                .with_root_certificates(certs)
                .with_no_client_auth()
        } else {
            return Err(ConnectionError(
                "Cannot connect: no certificate hash or root certificates provided".to_string(),
            ));
        };

        // Create Quinn client config
        let mut config = quinn::ClientConfig::new(Arc::new(
            quinn::crypto::rustls::QuicClientConfig::try_from(tls_config)
                .map_err(|e| ConnectionError(format!("QUIC config error: {e}")))?,
        ));

        // Configure transport options
        let mut transport_config = quinn::TransportConfig::default();
        transport_config.max_idle_timeout(
            options
                .max_idle_timeout
                .map(quinn::IdleTimeout::try_from)
                .transpose()
                .map_err(|_| ConnectionError("Invalid max_idle_timeout".to_string()))?,
        );
        transport_config.keep_alive_interval(options.keep_alive_interval);
        config.transport_config(Arc::new(transport_config));

        let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0);
        let endpoint = quinn::Endpoint::client(bind_addr)?;
        let conn = endpoint.connect_with(config, addr, server_name)?.await?;
        Ok(conn.into())
    }
}

#[async_trait]
impl ConnectionDriver for QuinnConnectionDriver {
    async fn open_uni(&self) -> Result<SendStream, ConnectionError> {
        let quinn_send = self.conn.open_uni().await?;
        let send_driver = QuinnSendStreamDriver::new(quinn_send);
        let send = SendStream::new(send_driver);
        Ok(send)
    }

    async fn open_bi(&self) -> Result<(SendStream, RecvStream), ConnectionError> {
        let (quinn_send, quinn_recv) = self.conn.open_bi().await?;
        let send_driver = QuinnSendStreamDriver::new(quinn_send);
        let recv_driver = QuinnRecvStreamDriver::new(quinn_recv);
        let send = SendStream::new(send_driver);
        let recv = RecvStream::new(recv_driver);
        Ok((send, recv))
    }

    async fn accept_uni(&self) -> Result<RecvStream, ConnectionError> {
        let quinn_recv = self.conn.accept_uni().await?;
        let recv_driver = QuinnRecvStreamDriver::new(quinn_recv);
        let recv = RecvStream::new(recv_driver);
        Ok(recv)
    }

    async fn accept_bi(&self) -> Result<(SendStream, RecvStream), ConnectionError> {
        let (quinn_send, quinn_recv) = self.conn.accept_bi().await?;
        let send_driver = QuinnSendStreamDriver::new(quinn_send);
        let recv_driver = QuinnRecvStreamDriver::new(quinn_recv);
        let send = SendStream::new(send_driver);
        let recv = RecvStream::new(recv_driver);
        Ok((send, recv))
    }

    async fn read_datagram(&self) -> Result<Bytes, ConnectionError> {
        let datagram = self.conn.read_datagram().await?;
        Ok(datagram)
    }

    async fn send_datagram(&self, data: Bytes) -> Result<(), SendDatagramError> {
        self.conn.send_datagram(data)?;
        Ok(())
    }

    async fn closed(&self) -> Result<(), ConnectionError> {
        match self.conn.closed().await {
            quinn::ConnectionError::LocallyClosed
            | quinn::ConnectionError::ApplicationClosed(_) => Ok(()),
            err => Err(err.into()),
        }
    }

    fn close(&self, error_code: u32, reason: &str) {
        let var_int = quinn::VarInt::from_u32(error_code);
        self.conn.close(var_int, reason.as_bytes());
    }

    fn max_datagram_size(&self) -> Option<usize> {
        self.conn.max_datagram_size()
    }

    async fn stats(&self) -> ConnectionStats {
        let stats = self.conn.stats();
        ConnectionStats {
            rtt: Some(stats.path.rtt),
            packets_lost: Some(stats.path.lost_packets),
        }
    }
}

#[derive(Debug)]
struct QuinnSendStreamDriver {
    send: quinn::SendStream,
}

impl QuinnSendStreamDriver {
    fn new(send: quinn::SendStream) -> Self {
        Self { send }
    }
}

#[async_trait]
impl SendStreamDriver for QuinnSendStreamDriver {
    async fn write(&mut self, buf: &[u8]) -> Result<usize, WriteError> {
        let size = self.send.write(buf).await?;
        Ok(size)
    }

    async fn write_all(&mut self, buf: &[u8]) -> Result<(), WriteError> {
        self.send.write_all(buf).await?;
        Ok(())
    }

    async fn close(&mut self) -> Result<(), ClosedStreamError> {
        self.send.finish()?;
        Ok(())
    }

    async fn abort(mut self: Box<Self>) -> Result<(), ClosedStreamError> {
        // Not async, but the trait requires the method to be async, because
        // other implementations might abort asynchronously
        self.send.reset(quinn::VarInt::from_u32(0))?;
        Ok(())
    }
}

#[derive(Debug)]
struct QuinnRecvStreamDriver {
    recv: quinn::RecvStream,
}

impl QuinnRecvStreamDriver {
    fn new(recv: quinn::RecvStream) -> Self {
        Self { recv }
    }
}

#[async_trait]
impl RecvStreamDriver for QuinnRecvStreamDriver {
    async fn read(&mut self, buf: &mut [u8]) -> Result<Option<usize>, ReadError> {
        let size = self.recv.read(buf).await?;
        Ok(size)
    }
}

impl From<quinn::ConnectionError> for ConnectionError {
    fn from(value: quinn::ConnectionError) -> Self {
        Self(value.to_string())
    }
}

impl From<quinn::ConnectError> for ConnectionError {
    fn from(value: quinn::ConnectError) -> Self {
        Self(value.to_string())
    }
}

impl From<quinn::ReadError> for ReadError {
    fn from(value: quinn::ReadError) -> Self {
        Self(value.to_string())
    }
}

impl From<quinn::WriteError> for WriteError {
    fn from(value: quinn::WriteError) -> Self {
        Self(value.to_string())
    }
}

impl From<quinn::SendDatagramError> for SendDatagramError {
    fn from(value: quinn::SendDatagramError) -> Self {
        Self(value.to_string())
    }
}

impl From<quinn::ClosedStream> for ClosedStreamError {
    fn from(_: quinn::ClosedStream) -> Self {
        Self
    }
}
