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
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use rustls_platform_verifier::ConfigVerifierExt;
use socket2::{Domain, Protocol, Socket, Type};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// ALPN protocol identifier for Kymux protocol over standard QUIC.
pub const KYMUX_ALPN: &[u8] = b"kymux";

/// Quinn congestion-controller factory accepted by the generic Kynet driver.
pub type CongestionControllerFactory =
    Arc<dyn quinn::congestion::ControllerFactory + Send + Sync + 'static>;

#[derive(Debug)]
struct DatagramPacerState {
    virtual_finish: tokio::time::Instant,
}

/// Optional application-side DATAGRAM pacer.
///
/// KyProto already awaits every `send_datagram()` call. Kynet can therefore
/// pace submissions before they enter Quinn without changing KyProto's media
/// packetization or FEC object model. The small burst allowance avoids a timer
/// wakeup for every individual datagram.
#[derive(Debug)]
pub struct DatagramPacer {
    target_bps: AtomicU64,
    burst: Duration,
    wire_overhead_bytes: usize,
    state: Mutex<DatagramPacerState>,
}

impl DatagramPacer {
    pub fn new(target_bps: u64, burst: Duration, wire_overhead_bytes: usize) -> Self {
        Self {
            target_bps: AtomicU64::new(target_bps),
            burst,
            wire_overhead_bytes,
            state: Mutex::new(DatagramPacerState {
                virtual_finish: tokio::time::Instant::now(),
            }),
        }
    }

    pub fn target_bps(&self) -> u64 {
        self.target_bps.load(Ordering::Acquire)
    }

    pub fn set_target_bps(&self, target_bps: u64) {
        self.target_bps.store(target_bps, Ordering::Release);
        self.state.lock().unwrap().virtual_finish = tokio::time::Instant::now();
    }

    fn reserve_deadline(
        &self,
        payload_bytes: usize,
        now: tokio::time::Instant,
    ) -> Option<tokio::time::Instant> {
        let target_bps = self.target_bps();
        if target_bps == 0 {
            return None;
        }

        let wire_bytes = payload_bytes.saturating_add(self.wire_overhead_bytes);
        let serialization_nanos = (wire_bytes as u128)
            .saturating_mul(8)
            .saturating_mul(1_000_000_000)
            .div_ceil(target_bps as u128)
            .min(u64::MAX as u128) as u64;
        let serialization = Duration::from_nanos(serialization_nanos);
        let deadline = {
            let mut state = self.state.lock().unwrap();
            if state.virtual_finish < now {
                state.virtual_finish = now;
            }
            state.virtual_finish += serialization;
            state
                .virtual_finish
                .checked_sub(self.burst)
                .unwrap_or(now)
                .max(now)
        };
        Some(deadline)
    }

    async fn wait(&self, payload_bytes: usize) {
        let now = tokio::time::Instant::now();
        if let Some(deadline) = self.reserve_deadline(payload_bytes, now)
            && deadline > now
        {
            tokio::time::sleep_until(deadline).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::DatagramPacer;
    use std::time::Duration;

    #[test]
    fn datagram_pacer_reserves_against_one_shared_timeline() {
        let pacer = DatagramPacer::new(8_000_000, Duration::ZERO, 0);
        let now = tokio::time::Instant::now();
        let first = pacer.reserve_deadline(1_000, now).unwrap();
        let second = pacer.reserve_deadline(1_000, now).unwrap();
        assert_eq!(first - now, Duration::from_millis(1));
        assert_eq!(second - now, Duration::from_millis(2));
    }

    #[test]
    fn datagram_pacer_permits_only_the_configured_burst() {
        let pacer = DatagramPacer::new(8_000_000, Duration::from_millis(2), 0);
        let now = tokio::time::Instant::now();
        assert_eq!(pacer.reserve_deadline(1_000, now), Some(now));
        assert_eq!(pacer.reserve_deadline(1_000, now), Some(now));
        assert_eq!(
            pacer.reserve_deadline(1_000, now),
            Some(now + Duration::from_millis(1))
        );
    }

    #[test]
    fn zero_rate_disables_pacing() {
        let pacer = DatagramPacer::new(0, Duration::from_millis(2), 0);
        assert_eq!(
            pacer.reserve_deadline(64 * 1024, tokio::time::Instant::now()),
            None
        );
    }
}

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
        use ring::digest::{SHA256, digest};
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
        Self::new(QuinnConnectionDriver::wrap(value, None))
    }
}

#[derive(Default)]
pub struct QuinnClientOptions {
    pub max_idle_timeout: Option<Duration>,
    pub keep_alive_interval: Option<Duration>,
    /// Maximum complete UDP payload accepted and probed by QUIC, excluding
    /// the outer IP and UDP headers. This allows an application to account
    /// for an encapsulating transport whose virtual interface hides a smaller
    /// physical datagram boundary.
    pub max_udp_payload_size: Option<u16>,
    /// SHA-256 hash of the expected server certificate (hex encoded).
    /// If provided, the server certificate will be validated by comparing its hash
    /// instead of using CA chain validation. This is similar to WebTransport's
    /// serverCertificateHashes option.
    pub certificate_hash: Option<String>,
    /// Optional application-selected Quinn congestion controller.
    pub congestion_controller_factory: Option<CongestionControllerFactory>,
    /// Optional application-side DATAGRAM pacer shared by the connection.
    pub datagram_pacer: Option<Arc<DatagramPacer>>,
}

#[derive(Debug)]
pub(crate) struct QuinnConnectionDriver {
    conn: quinn::Connection,
    datagram_pacer: Option<Arc<DatagramPacer>>,
}

impl QuinnConnectionDriver {
    pub(crate) fn wrap(
        conn: quinn::Connection,
        datagram_pacer: Option<Arc<DatagramPacer>>,
    ) -> Self {
        Self {
            conn,
            datagram_pacer,
        }
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
        if let Some(factory) = &options.congestion_controller_factory {
            transport_config.congestion_controller_factory(factory.clone());
        }
        if let Some(max_udp_payload_size) = options.max_udp_payload_size {
            let mut mtu_discovery = quinn::MtuDiscoveryConfig::default();
            mtu_discovery.upper_bound(max_udp_payload_size);
            transport_config.mtu_discovery_config(Some(mtu_discovery));
        }
        config.transport_config(Arc::new(transport_config));

        let bind_ip_addr = match &addr {
            SocketAddr::V4(_) => IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            SocketAddr::V6(_) => IpAddr::V6(Ipv6Addr::UNSPECIFIED),
        };

        let bind_addr = SocketAddr::new(bind_ip_addr, 0);
        let socket = Socket::new(
            Domain::for_address(bind_addr),
            Type::DGRAM,
            Some(Protocol::UDP),
        )?;
        if bind_addr.is_ipv6()
            && let Err(error) = socket.set_only_v6(false)
        {
            log::debug!("Unable to make QUIC client socket dual-stack: {error}");
        }
        socket.bind(&bind_addr.into())?;

        let mut endpoint_config = quinn::EndpointConfig::default();
        if let Some(max_udp_payload_size) = options.max_udp_payload_size {
            endpoint_config
                .max_udp_payload_size(max_udp_payload_size)
                .map_err(|error| {
                    ConnectionError(format!("Invalid maximum UDP payload size: {error}"))
                })?;
        }
        let endpoint = quinn::Endpoint::new(
            endpoint_config,
            None,
            socket.into(),
            Arc::new(quinn::TokioRuntime),
        )?;
        let conn = endpoint.connect_with(config, addr, server_name)?.await?;
        Ok(Connection::new(Self::wrap(
            conn,
            options.datagram_pacer.clone(),
        )))
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
        if let Some(pacer) = &self.datagram_pacer {
            pacer.wait(data.len()).await;
        }
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

impl AsyncWrite for QuinnSendStreamDriver {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        AsyncWrite::poll_write(Pin::new(&mut self.send), cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.send).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.send).poll_shutdown(cx)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.send).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        Pin::new(&self.send).is_write_vectored()
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

impl AsyncRead for QuinnRecvStreamDriver {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.recv).poll_read(cx, buf)
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
