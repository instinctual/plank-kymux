// Project Kyber: quinn.rs
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

use crate::error::*;
use crate::{
    Connection, ConnectionDriver, ConnectionStats, RecvStream, RecvStreamDriver, SendStream,
    SendStreamDriver,
};

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use rustls_platform_verifier::ConfigVerifierExt;

/// ALPN protocol identifier for Kymux protocol over standard QUIC.
pub const KYMUX_ALPN: &[u8] = b"kymux";

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
        tls_config: Option<rustls::ClientConfig>,
        options: &QuinnClientOptions,
    ) -> Result<Connection, ConnectionError> {
        let mut tls_config = if let Some(ref hash) = options.certificate_hash {
            let verifier = HashCertVerifier::new(hash)?;
            rustls::ClientConfig::builder()
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(verifier))
                .with_no_client_auth()
        } else if let Some(tls_config) = tls_config {
            tls_config
        } else {
            rustls::ClientConfig::with_platform_verifier()
                .map_err(|e| ConnectionError(format!("Platform verifier error: {e}")))?
        };

        tls_config.alpn_protocols = vec![KYMUX_ALPN.to_vec()];

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

        let bind_ip_addr = match &addr {
            SocketAddr::V4(_) => IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            SocketAddr::V6(_) => IpAddr::V6(Ipv6Addr::UNSPECIFIED),
        };

        let bind_addr = SocketAddr::new(bind_ip_addr, 0);
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

    async fn finish(&mut self) -> Result<(), ClosedStreamError> {
        self.send.finish()?;
        self.send.stopped().await.map_err(|_| ClosedStreamError)?;
        Ok(())
    }

    fn reset(&mut self) {
        // ignore error if the stream is already closed
        let _ = self.send.reset(quinn::VarInt::from_u32(0));
    }

    async fn closed(&mut self) -> Result<(), ConnectionError> {
        // stopped() requires a &mut self
        self.send
            .stopped()
            .await
            .map_err(|e| ConnectionError(format!("Stopped error: {e}")))?;
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

    fn stop(&mut self) {
        // ignore error if the stream is already closed
        let _ = self.recv.stop(quinn::VarInt::from_u32(0));
    }

    async fn closed(&mut self) -> Result<(), ConnectionError> {
        // received_reset() requires a &mut self
        self.recv
            .received_reset()
            .await
            .map_err(|e| ConnectionError(format!("Stopped error: {e}")))?;
        Ok(())
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
