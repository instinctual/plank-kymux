// Project Kyber: wtransport.rs
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

use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use rustls_platform_verifier::ConfigVerifierExt;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

impl From<wtransport::Connection> for Connection {
    fn from(value: wtransport::Connection) -> Self {
        Self::new(WTransportConnectionDriver::wrap(value))
    }
}

#[derive(Default)]
pub struct WTransportClientOptions {
    pub max_idle_timeout: Option<Duration>,
    pub keep_alive_interval: Option<Duration>,
}

#[derive(Debug)]
pub(crate) struct WTransportConnectionDriver {
    conn: wtransport::Connection,
}

impl WTransportConnectionDriver {
    fn wrap(conn: wtransport::Connection) -> Self {
        Self { conn }
    }

    pub async fn connect(
        url: &str,
        tls_config: Option<rustls::ClientConfig>,
        options: &WTransportClientOptions,
    ) -> Result<Connection, ConnectionError> {
        let mut tls_config = if let Some(tls_config) = tls_config {
            tls_config
        } else {
            rustls::ClientConfig::with_platform_verifier()
                .map_err(|e| ConnectionError(format!("Platform verifier error: {e}")))?
        };
        tls_config.alpn_protocols = vec![wtransport_proto::WEBTRANSPORT_ALPN.to_vec()];

        // with_bind_default() enables dual-stack
        // Ref: https://docs.rs/wtransport/0.6.1/wtransport/config/struct.ClientConfigBuilder.html#method.with_bind_default
        let config = wtransport::ClientConfig::builder()
            .with_bind_default()
            .with_custom_tls(tls_config)
            .max_idle_timeout(options.max_idle_timeout)?
            .keep_alive_interval(options.keep_alive_interval)
            .build();

        let endpoint = wtransport::Endpoint::client(config)?;
        let conn = endpoint.connect(url).await?;
        Ok(conn.into())
    }
}

#[async_trait]
impl ConnectionDriver for WTransportConnectionDriver {
    async fn open_uni(&self) -> Result<SendStream, ConnectionError> {
        let wt_send = self.conn.open_uni().await?.await?;
        let send_driver = WTransportSendStreamDriver::new(wt_send);
        let send = SendStream::new(send_driver);
        Ok(send)
    }

    async fn open_bi(&self) -> Result<(SendStream, RecvStream), ConnectionError> {
        let (wt_send, wt_recv) = self.conn.open_bi().await?.await?;
        let send_driver = WTransportSendStreamDriver::new(wt_send);
        let recv_driver = WTransportRecvStreamDriver::new(wt_recv);
        let send = SendStream::new(send_driver);
        let recv = RecvStream::new(recv_driver);
        Ok((send, recv))
    }

    async fn accept_uni(&self) -> Result<RecvStream, ConnectionError> {
        let wt_recv = self.conn.accept_uni().await?;
        let recv_driver = WTransportRecvStreamDriver::new(wt_recv);
        let recv = RecvStream::new(recv_driver);
        Ok(recv)
    }

    async fn accept_bi(&self) -> Result<(SendStream, RecvStream), ConnectionError> {
        let (wt_send, wt_recv) = self.conn.accept_bi().await?;
        let send_driver = WTransportSendStreamDriver::new(wt_send);
        let recv_driver = WTransportRecvStreamDriver::new(wt_recv);
        let send = SendStream::new(send_driver);
        let recv = RecvStream::new(recv_driver);
        Ok((send, recv))
    }

    async fn read_datagram(&self) -> Result<Bytes, ConnectionError> {
        let datagram = self.conn.receive_datagram().await?;
        let bytes = Bytes::copy_from_slice(&datagram);
        Ok(bytes)
    }

    async fn send_datagram(&self, data: Bytes) -> Result<(), SendDatagramError> {
        self.conn.send_datagram(data)?;
        Ok(())
    }

    async fn closed(&self) -> Result<(), ConnectionError> {
        match self.conn.closed().await {
            wtransport::error::ConnectionError::LocallyClosed
            | wtransport::error::ConnectionError::ApplicationClosed(_) => Ok(()),
            err => Err(err.into()),
        }
    }

    fn close(&self, error_code: u32, reason: &str) {
        self.conn.close(
            wtransport_proto::varint::VarInt::from(error_code),
            reason.as_bytes(),
        );
    }

    fn max_datagram_size(&self) -> Option<usize> {
        self.conn.max_datagram_size()
    }

    async fn stats(&self) -> ConnectionStats {
        let stats = self.conn.quic_connection().stats();
        ConnectionStats {
            rtt: Some(stats.path.rtt),
            packets_lost: Some(stats.path.lost_packets),
        }
    }
}

#[derive(Debug)]
struct WTransportSendStreamDriver {
    send: wtransport::SendStream,
}

impl WTransportSendStreamDriver {
    fn new(send: wtransport::SendStream) -> Self {
        Self { send }
    }
}

#[async_trait]
impl SendStreamDriver for WTransportSendStreamDriver {
    async fn finish(&mut self) -> Result<(), ClosedStreamError> {
        self.send.finish().await.map_err(|_| ClosedStreamError)
    }

    fn reset(&mut self) {
        // ignore error if the stream is already closed
        let _ = self
            .send
            .reset(wtransport_proto::varint::VarInt::from_u32(0));
    }

    async fn closed(&mut self) -> Result<(), ConnectionError> {
        self.send
            .quic_stream_mut()
            .stopped()
            .await
            .map_err(|e| ConnectionError(format!("Stopped error: {e}")))?;
        Ok(())
    }
}

impl AsyncWrite for WTransportSendStreamDriver {
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
struct WTransportRecvStreamDriver {
    recv: wtransport::RecvStream,
}

impl WTransportRecvStreamDriver {
    fn new(recv: wtransport::RecvStream) -> Self {
        Self { recv }
    }
}

#[async_trait]
impl RecvStreamDriver for WTransportRecvStreamDriver {
    fn stop(&mut self) {
        // ignore error if the stream is already closed
        let _ = self.recv.quic_stream_mut().stop(quinn::VarInt::from_u32(0));
    }

    async fn closed(&mut self) -> Result<(), ConnectionError> {
        self.recv
            .quic_stream_mut()
            .received_reset()
            .await
            .map_err(|e| ConnectionError(format!("Stopped error: {e}")))?;
        Ok(())
    }
}

impl AsyncRead for WTransportRecvStreamDriver {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.recv).poll_read(cx, buf)
    }
}

impl From<wtransport::error::ConnectionError> for ConnectionError {
    fn from(value: wtransport::error::ConnectionError) -> Self {
        Self(value.to_string())
    }
}

impl From<wtransport::error::ConnectingError> for ConnectionError {
    fn from(value: wtransport::error::ConnectingError) -> Self {
        Self(value.to_string())
    }
}

impl From<wtransport::error::StreamOpeningError> for ConnectionError {
    fn from(value: wtransport::error::StreamOpeningError) -> Self {
        Self(value.to_string())
    }
}

impl From<wtransport::error::SendDatagramError> for SendDatagramError {
    fn from(value: wtransport::error::SendDatagramError) -> Self {
        Self(value.to_string())
    }
}

impl From<wtransport::config::InvalidIdleTimeout> for ConnectionError {
    fn from(_: wtransport::config::InvalidIdleTimeout) -> Self {
        Self("Invalid max_idle_timeout".to_string())
    }
}
