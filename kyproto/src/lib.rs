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

#[cfg(all(not(feature = "js"), not(feature = "tokio-rt")))]
compile_error!("No feature selected, pass either --features=js or --features=tokio-rt");

use std::sync::atomic::{AtomicU16, Ordering};

use control::{Control, ReadyNotifier};
use router::{KyChannel, Router};

use async_trait::async_trait;
use kymux_types as types;
use kyutil::*;

pub use auth::{ClientAuth, UnauthenticatedConnection};
#[cfg(all(
    any(feature = "kynet-quinn", feature = "kynet-wtransport"),
    not(target_family = "wasm")
))]
pub use connection::Server;
pub use kymux_types::{ProtocolEndpoint, ProtocolError};
pub use kyutil::DecodeHexError;
pub use protocol::clock_sync::{ClockSyncClientProtocol, ClockSyncServerProtocol};
pub use protocol::{AudioProtocol, VideoProtocol};

pub use kynet::error::{ConnectionError, ReadExactError};
pub use kynet::init_crypto;
pub use kynet::ConnectionStats;

mod auth;
pub mod clock;
mod connection;
mod control;
mod protocol;
mod router;
pub mod runtime;
mod task;

#[cfg(all(
    any(feature = "kynet-quinn", feature = "kynet-wtransport"),
    not(target_family = "wasm")
))]
pub use {kynet::cert, std::net::SocketAddr};

#[cfg(all(feature = "kynet-quinn", not(target_family = "wasm")))]
pub use connection::quinn;

#[cfg(all(feature = "kynet-webtransport-js", target_family = "wasm"))]
pub use connection::webtransport_js;

#[cfg(all(feature = "kynet-wtransport", not(target_family = "wasm")))]
pub use connection::wtransport;

#[cfg(all(feature = "kynet-quinn", not(target_family = "wasm")))]
pub use connection::common;

pub struct VideoServerEndpoint {
    id: u16,
    ready_notifier: ReadyNotifier,
    ky_channel: KyChannel,
    video_protocol: VideoProtocol,
    protocol_stats: KyArc<KyMutex<ProtocolStats>>,
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ProtocolEndpoint for VideoServerEndpoint {
    type Protocol = types::VideoServerProtocol;

    fn id(&self) -> u16 {
        self.id
    }

    async fn ready_boxed(self: Box<Self>) -> Result<Self::Protocol, ProtocolError> {
        self.ready_notifier
            .ready()
            .await
            .map_err(ProtocolError::new)?;
        let send = protocol::start_video_protocol_send(
            self.ky_channel,
            self.video_protocol,
            &self.protocol_stats,
        )
        .await?;
        Ok(Self::Protocol { send })
    }
}

pub struct VideoClientEndpoint {
    id: u16,
    ready_notifier: ReadyNotifier,
    ky_channel: KyChannel,
    video_protocol: VideoProtocol,
    protocol_stats: KyArc<KyMutex<ProtocolStats>>,
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ProtocolEndpoint for VideoClientEndpoint {
    type Protocol = types::VideoClientProtocol;

    fn id(&self) -> u16 {
        self.id
    }

    async fn ready_boxed(self: Box<Self>) -> Result<Self::Protocol, ProtocolError> {
        self.ready_notifier
            .ready()
            .await
            .map_err(ProtocolError::new)?;
        let recv = protocol::start_video_protocol_recv(
            self.ky_channel,
            self.video_protocol,
            &self.protocol_stats,
        )
        .await?;
        Ok(Self::Protocol { recv })
    }
}

pub struct AudioServerEndpoint {
    id: u16,
    ready_notifier: ReadyNotifier,
    ky_channel: KyChannel,
    audio_protocol: AudioProtocol,
    protocol_stats: KyArc<KyMutex<ProtocolStats>>,
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ProtocolEndpoint for AudioServerEndpoint {
    type Protocol = types::AudioServerProtocol;

    fn id(&self) -> u16 {
        self.id
    }

    async fn ready_boxed(self: Box<Self>) -> Result<Self::Protocol, ProtocolError> {
        self.ready_notifier
            .ready()
            .await
            .map_err(ProtocolError::new)?;
        let send = protocol::start_audio_protocol_send(
            self.ky_channel,
            self.audio_protocol,
            &self.protocol_stats,
        )
        .await?;
        Ok(Self::Protocol { send })
    }
}

pub struct AudioClientEndpoint {
    id: u16,
    ready_notifier: ReadyNotifier,
    ky_channel: KyChannel,
    audio_protocol: AudioProtocol,
    protocol_stats: KyArc<KyMutex<ProtocolStats>>,
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ProtocolEndpoint for AudioClientEndpoint {
    type Protocol = types::AudioClientProtocol;

    fn id(&self) -> u16 {
        self.id
    }

    async fn ready_boxed(self: Box<Self>) -> Result<Self::Protocol, ProtocolError> {
        self.ready_notifier
            .ready()
            .await
            .map_err(ProtocolError::new)?;
        let recv = protocol::start_audio_protocol_recv(
            self.ky_channel,
            self.audio_protocol,
            &self.protocol_stats,
        )
        .await?;
        Ok(Self::Protocol { recv })
    }
}

pub struct DataEndpoint {
    id: u16,
    ready_notifier: ReadyNotifier,
    ky_channel: KyChannel,
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ProtocolEndpoint for DataEndpoint {
    type Protocol = types::DataProtocol;

    fn id(&self) -> u16 {
        self.id
    }

    async fn ready_boxed(self: Box<Self>) -> Result<Self::Protocol, ProtocolError> {
        self.ready_notifier
            .ready()
            .await
            .map_err(ProtocolError::new)?;
        let (send, recv) = protocol::start_data_protocol(self.ky_channel).await?;
        Ok(Self::Protocol { send, recv })
    }
}

pub struct InputEndpoint {
    id: u16,
    ready_notifier: ReadyNotifier,
    ky_channel: KyChannel,
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ProtocolEndpoint for InputEndpoint {
    type Protocol = types::InputProtocol;

    fn id(&self) -> u16 {
        self.id
    }

    async fn ready_boxed(self: Box<Self>) -> Result<Self::Protocol, ProtocolError> {
        self.ready_notifier
            .ready()
            .await
            .map_err(ProtocolError::new)?;
        let (send, recv) = protocol::start_input_protocol(self.ky_channel).await?;
        Ok(Self::Protocol { send, recv })
    }
}

pub struct ClockSyncServerEndpoint {
    id: u16,
    ready_notifier: ReadyNotifier,
    ky_channel: KyChannel,
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ProtocolEndpoint for ClockSyncServerEndpoint {
    type Protocol = ClockSyncServerProtocol;

    fn id(&self) -> u16 {
        self.id
    }

    async fn ready_boxed(self: Box<Self>) -> Result<Self::Protocol, ProtocolError> {
        self.ready_notifier
            .ready()
            .await
            .map_err(ProtocolError::new)?;
        Ok(ClockSyncServerProtocol::new(self.ky_channel))
    }
}

pub struct ClockSyncClientEndpoint {
    id: u16,
    ready_notifier: ReadyNotifier,
    ky_channel: KyChannel,
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ProtocolEndpoint for ClockSyncClientEndpoint {
    type Protocol = ClockSyncClientProtocol;

    fn id(&self) -> u16 {
        self.id
    }

    async fn ready_boxed(self: Box<Self>) -> Result<Self::Protocol, ProtocolError> {
        self.ready_notifier
            .ready()
            .await
            .map_err(ProtocolError::new)?;
        Ok(ClockSyncClientProtocol::new(self.ky_channel))
    }
}

pub struct MetricsServerEndpoint {
    id: u16,
    ready_notifier: ReadyNotifier,
    ky_channel: KyChannel,
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ProtocolEndpoint for MetricsServerEndpoint {
    type Protocol = types::MetricsServerProtocol;

    fn id(&self) -> u16 {
        self.id
    }

    async fn ready_boxed(self: Box<Self>) -> Result<Self::Protocol, ProtocolError> {
        self.ready_notifier
            .ready()
            .await
            .map_err(ProtocolError::new)?;
        let send = protocol::start_metrics_protocol_send(self.ky_channel).await?;
        Ok(Self::Protocol { send })
    }
}

pub struct MetricsClientEndpoint {
    id: u16,
    ready_notifier: ReadyNotifier,
    ky_channel: KyChannel,
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ProtocolEndpoint for MetricsClientEndpoint {
    type Protocol = types::MetricsClientProtocol;

    fn id(&self) -> u16 {
        self.id
    }

    async fn ready_boxed(self: Box<Self>) -> Result<Self::Protocol, ProtocolError> {
        self.ready_notifier
            .ready()
            .await
            .map_err(ProtocolError::new)?;
        let recv = protocol::start_metrics_protocol_recv(self.ky_channel).await?;
        Ok(Self::Protocol { recv })
    }
}
///
/// Stats filled by protocol implementations
#[derive(Debug, Clone, Default)]
pub struct ProtocolStats {
    /// The number of (unreliable) packets not received or received too late
    pub dropped_packets: Option<u64>,
}

const INITIATOR_SERVER: u16 = 0;
const INITIATOR_CLIENT: u16 = 1;

pub struct Connection {
    conn: kynet::Connection,
    router: Router,
    control: Control,

    // Endpoint ID generation: ids can be allocated by both sides.
    // Like Quic streams, use u16 parity to avoid collision.
    //
    // The parity bit is defined by INITIATOR_SERVER/INITIATOR_CLIENT.
    //
    // quinn example: https://github.com/quinn-rs/quinn/blob/e652b6d999f053ffe21eeea247854882ae480281/quinn-proto/src/lib.rs#L230
    next_endpoint_index: AtomicU16,
    initiator: u16,

    protocol_stats: KyArc<KyMutex<ProtocolStats>>,
}

impl Connection {
    fn new(conn: kynet::Connection, router: Router, control: Control, initiator: u16) -> Self {
        Self {
            conn,
            router,
            control,
            initiator,
            next_endpoint_index: AtomicU16::new(0),
            protocol_stats: KyArc::new(KyMutex::new(ProtocolStats::default())),
        }
    }

    pub async fn connect(conn: kynet::Connection) -> Result<Self, ConnectionError> {
        let (mut tx, rx) = conn.open_bi().await?;

        // Force a packet to be sent so that accept_bi() can detect bi-stream opening
        tx.write(&[0])
            .await
            .map_err(|_| ConnectionError("Dummy byte write failed".to_string()))?;

        let control = Control::start(tx, rx);
        let router = Router::start(conn.clone());
        Ok(Self::new(conn, router, control, INITIATOR_CLIENT))
    }

    pub async fn connect_with_auth(
        conn: kynet::Connection,
        auth: &ClientAuth,
    ) -> Result<Self, ConnectionError> {
        let (mut tx, mut rx) = conn.open_bi().await?;

        // Send auth
        assert!(auth.token().len() <= auth::TOKEN_MAX_LEN);
        let buf = rmp_serde::to_vec(&auth).expect("Failed to serialize ClientAuth");
        let len = u16::try_from(buf.len()).expect("ClientAuth too big");

        tx.write_all(&len.to_be_bytes())
            .await
            .map_err(|_| ConnectionError("Could not write auth ClientAuth size".to_string()))?;

        tx.write_all(&buf)
            .await
            .map_err(|_| ConnectionError("Could not write ClientAuth".to_string()))?;

        // The server writes 1 byte to confirm authentication, or closes the connection on failure
        rx.read(&mut [0])
            .await
            .map_err(|_| ConnectionError("Authentication confirmation failed".to_string()))?;

        let control = Control::start(tx, rx);
        let router = Router::start(conn.clone());
        Ok(Self {
            conn,
            router,
            control,
            initiator: INITIATOR_CLIENT,
            next_endpoint_index: AtomicU16::new(0),
            protocol_stats: KyArc::new(KyMutex::new(ProtocolStats::default())),
        })
    }

    pub async fn accept(conn: kynet::Connection) -> Result<Self, ConnectionError> {
        let (tx, mut rx) = conn.accept_bi().await?;

        // Consume the dummy byte used to detect the bi-stream immediately
        rx.read(&mut [0])
            .await
            .map_err(|_| ConnectionError("Dummy byte read failed".to_string()))?;

        let control = Control::start(tx, rx);
        let router = Router::start(conn.clone());
        Ok(Self::new(conn, router, control, INITIATOR_SERVER))
    }

    pub async fn accept_with_auth(
        conn: kynet::Connection,
    ) -> Result<UnauthenticatedConnection, ConnectionError> {
        let (tx, mut rx) = conn.accept_bi().await?;

        let mut buf = [0u8; std::mem::size_of::<u16>()];
        rx.read_exact(&mut buf)
            .await
            .map_err(|_| ConnectionError("Failed to read ClientAuth size".to_string()))?;

        let len = u16::from_be_bytes(buf);
        if len as usize > auth::TOKEN_MAX_LEN {
            auth::reject_authentication(&conn);
            return Err(ConnectionError(
                "ClientAuth size too big. Reject it".to_string(),
            ));
        }

        let mut buf = vec![0u8; len as usize];
        rx.read_exact(&mut buf)
            .await
            .map_err(|_| ConnectionError("Failed to read ClientAuth".to_string()))?;

        let auth = rmp_serde::from_slice(&buf)
            .map_err(|_| ConnectionError("Failed to deserialize ClientAuth".to_string()))?;

        Ok(UnauthenticatedConnection::new(conn, auth, tx, rx))
    }

    #[cfg(all(feature = "kynet-quinn", not(target_family = "wasm")))]
    pub async fn quinn_connect(
        addr: SocketAddr,
        server_name: &str,
        certs: Option<cert::RootCertStore>,
        options: &quinn::QuinnClientOptions,
    ) -> Result<Self, ConnectionError> {
        let conn = kynet::Connection::quinn_connect(addr, server_name, certs, options).await?;
        Self::connect(conn).await
    }

    #[cfg(all(feature = "kynet-quinn", not(target_family = "wasm")))]
    pub async fn quinn_connect_with_auth(
        addr: SocketAddr,
        server_name: &str,
        certs: Option<cert::RootCertStore>,
        options: &quinn::QuinnClientOptions,
        auth: &ClientAuth,
    ) -> Result<Self, ConnectionError> {
        let conn = kynet::Connection::quinn_connect(addr, server_name, certs, options).await?;
        Self::connect_with_auth(conn, auth).await
    }

    #[cfg(all(feature = "kynet-wtransport", not(target_family = "wasm")))]
    pub async fn wtransport_connect(
        url: &str,
        certs: Option<cert::RootCertStore>,
        options: &wtransport::WTransportClientOptions,
    ) -> Result<Self, ConnectionError> {
        let conn = kynet::Connection::wtransport_connect(url, certs, options).await?;
        Self::connect(conn).await
    }

    #[cfg(all(feature = "kynet-wtransport", not(target_family = "wasm")))]
    pub async fn wtransport_connect_with_auth(
        url: &str,
        certs: Option<cert::RootCertStore>,
        options: &wtransport::WTransportClientOptions,
        auth: &ClientAuth,
    ) -> Result<Self, ConnectionError> {
        let conn = kynet::Connection::wtransport_connect(url, certs, options).await?;
        Self::connect_with_auth(conn, auth).await
    }

    #[cfg(all(feature = "kynet-webtransport-js", target_family = "wasm"))]
    pub async fn webtransport_js_connect(
        url: &str,
        options: &webtransport_js::WebTransportJSOptions,
    ) -> Result<Self, ConnectionError> {
        let conn = kynet::Connection::webtransport_js_connect(url, options).await?;
        Self::connect(conn).await
    }

    #[cfg(all(feature = "kynet-webtransport-js", target_family = "wasm"))]
    pub async fn webtransport_js_connect_with_auth(
        url: &str,
        options: &webtransport_js::WebTransportJSOptions,
        auth: &ClientAuth,
    ) -> Result<Self, ConnectionError> {
        let conn = kynet::Connection::webtransport_js_connect(url, options).await?;
        Self::connect_with_auth(conn, auth).await
    }

    #[cfg(all(feature = "kynet-quinn", not(target_family = "wasm")))]
    pub fn common_start_server_on_addr(
        addr: SocketAddr,
        cert_chain: Vec<cert::Certificate>,
        key: cert::PrivateKey,
        options: &common::CommonServerOptions,
    ) -> Result<common::CommonServer, ConnectionError> {
        common::CommonServer::start_on_addr(addr, cert_chain, key, options)
    }

    fn get_endpoint_id(&self, id: Option<u16>) -> u16 {
        if let Some(id) = id {
            id
        } else {
            let index = self.next_endpoint_index.fetch_add(1, Ordering::Relaxed);
            (index << 1) | self.initiator
        }
    }

    pub async fn register_video_endpoint(
        &self,
        id: Option<u16>,
        video_protocol: VideoProtocol,
    ) -> Result<VideoServerEndpoint, ProtocolError> {
        let id = self.get_endpoint_id(id);
        let ready_notifier = self
            .control
            .register_ready_notifier(id)
            .map_err(ProtocolError::new)?;
        self.control
            .register_endpoint(id)
            .await
            .map_err(ProtocolError::new)?;
        let ky_channel = self.router.register(id).map_err(ProtocolError::new)?;
        let protocol_stats = self.protocol_stats.clone();
        let endpoint = VideoServerEndpoint {
            id,
            ready_notifier,
            ky_channel,
            video_protocol,
            protocol_stats,
        };
        Ok(endpoint)
    }

    pub fn connect_video_endpoint(
        &self,
        id: u16,
        video_protocol: VideoProtocol,
    ) -> Result<VideoClientEndpoint, ProtocolError> {
        let ready_notifier = self
            .control
            .register_ready_notifier(id)
            .map_err(ProtocolError::new)?;
        let ky_channel = self.router.register(id).map_err(ProtocolError::new)?;
        let protocol_stats = self.protocol_stats.clone();
        let endpoint = VideoClientEndpoint {
            id,
            ready_notifier,
            ky_channel,
            video_protocol,
            protocol_stats,
        };
        Ok(endpoint)
    }

    pub async fn register_audio_endpoint(
        &self,
        id: Option<u16>,
        audio_protocol: AudioProtocol,
    ) -> Result<AudioServerEndpoint, ProtocolError> {
        let id = self.get_endpoint_id(id);
        let ready_notifier = self
            .control
            .register_ready_notifier(id)
            .map_err(ProtocolError::new)?;
        self.control
            .register_endpoint(id)
            .await
            .map_err(ProtocolError::new)?;
        let ky_channel = self.router.register(id).map_err(ProtocolError::new)?;
        let protocol_stats = self.protocol_stats.clone();
        let endpoint = AudioServerEndpoint {
            id,
            ready_notifier,
            ky_channel,
            audio_protocol,
            protocol_stats,
        };
        Ok(endpoint)
    }

    pub fn connect_audio_endpoint(
        &self,
        id: u16,
        audio_protocol: AudioProtocol,
    ) -> Result<AudioClientEndpoint, ProtocolError> {
        let ready_notifier = self
            .control
            .register_ready_notifier(id)
            .map_err(ProtocolError::new)?;
        let ky_channel = self.router.register(id).map_err(ProtocolError::new)?;
        let protocol_stats = self.protocol_stats.clone();
        let endpoint = AudioClientEndpoint {
            id,
            ready_notifier,
            ky_channel,
            audio_protocol,
            protocol_stats,
        };
        Ok(endpoint)
    }

    pub async fn register_data_endpoint(
        &self,
        id: Option<u16>,
    ) -> Result<DataEndpoint, ProtocolError> {
        let id = self.get_endpoint_id(id);
        let ready_notifier = self
            .control
            .register_ready_notifier(id)
            .map_err(ProtocolError::new)?;
        self.control
            .register_endpoint(id)
            .await
            .map_err(ProtocolError::new)?;
        let ky_channel = self.router.register(id).map_err(ProtocolError::new)?;
        let endpoint = DataEndpoint {
            id,
            ready_notifier,
            ky_channel,
        };
        Ok(endpoint)
    }

    pub fn connect_data_endpoint(&self, id: u16) -> Result<DataEndpoint, ProtocolError> {
        let ready_notifier = self
            .control
            .register_ready_notifier(id)
            .map_err(ProtocolError::new)?;
        let ky_channel = self.router.register(id).map_err(ProtocolError::new)?;
        let endpoint = DataEndpoint {
            id,
            ready_notifier,
            ky_channel,
        };
        Ok(endpoint)
    }

    pub async fn register_input_endpoint(
        &self,
        id: Option<u16>,
    ) -> Result<InputEndpoint, ProtocolError> {
        let id = self.get_endpoint_id(id);
        let ready_notifier = self
            .control
            .register_ready_notifier(id)
            .map_err(ProtocolError::new)?;
        self.control
            .register_endpoint(id)
            .await
            .map_err(ProtocolError::new)?;
        let ky_channel = self.router.register(id).map_err(ProtocolError::new)?;
        let endpoint = InputEndpoint {
            id,
            ready_notifier,
            ky_channel,
        };
        Ok(endpoint)
    }

    pub fn connect_input_endpoint(&self, id: u16) -> Result<InputEndpoint, ProtocolError> {
        let ready_notifier = self
            .control
            .register_ready_notifier(id)
            .map_err(ProtocolError::new)?;
        let ky_channel = self.router.register(id).map_err(ProtocolError::new)?;
        let endpoint = InputEndpoint {
            id,
            ready_notifier,
            ky_channel,
        };
        Ok(endpoint)
    }

    pub async fn register_clock_sync_endpoint(
        &self,
        id: Option<u16>,
    ) -> Result<ClockSyncServerEndpoint, ProtocolError> {
        let id = self.get_endpoint_id(id);
        let ready_notifier = self
            .control
            .register_ready_notifier(id)
            .map_err(ProtocolError::new)?;
        self.control
            .register_endpoint(id)
            .await
            .map_err(ProtocolError::new)?;
        let ky_channel = self.router.register(id).map_err(ProtocolError::new)?;
        let endpoint = ClockSyncServerEndpoint {
            id,
            ready_notifier,
            ky_channel,
        };
        Ok(endpoint)
    }

    pub fn connect_clock_sync_endpoint(
        &self,
        id: u16,
    ) -> Result<ClockSyncClientEndpoint, ProtocolError> {
        let ready_notifier = self
            .control
            .register_ready_notifier(id)
            .map_err(ProtocolError::new)?;
        let ky_channel = self.router.register(id).map_err(ProtocolError::new)?;
        let endpoint = ClockSyncClientEndpoint {
            id,
            ready_notifier,
            ky_channel,
        };
        Ok(endpoint)
    }

    pub async fn register_metrics_endpoint(
        &self,
        id: Option<u16>,
    ) -> Result<MetricsServerEndpoint, ProtocolError> {
        let id = self.get_endpoint_id(id);
        let ready_notifier = self
            .control
            .register_ready_notifier(id)
            .map_err(ProtocolError::new)?;
        self.control
            .register_endpoint(id)
            .await
            .map_err(ProtocolError::new)?;
        let ky_channel = self.router.register(id).map_err(ProtocolError::new)?;
        let endpoint = MetricsServerEndpoint {
            id,
            ready_notifier,
            ky_channel,
        };
        Ok(endpoint)
    }

    pub fn connect_metrics_endpoint(
        &self,
        id: u16,
    ) -> Result<MetricsClientEndpoint, ProtocolError> {
        let ready_notifier = self
            .control
            .register_ready_notifier(id)
            .map_err(ProtocolError::new)?;
        let ky_channel = self.router.register(id).map_err(ProtocolError::new)?;
        let endpoint = MetricsClientEndpoint {
            id,
            ready_notifier,
            ky_channel,
        };
        Ok(endpoint)
    }

    pub fn close(&self) {
        self.conn.close(0, "Closed by Kyproto user");
    }

    pub async fn closed(&self) -> Result<(), ConnectionError> {
        self.conn.closed().await
    }

    pub async fn connection_stats(&self) -> ConnectionStats {
        KyChannel::connection_stats_(&self.conn).await
    }

    pub fn protocol_stats(&self) -> ProtocolStats {
        self.protocol_stats.lock().clone()
    }

    // Allow to retrieve stats without a reference to the KyProto instance
    pub fn stats_provider(&self) -> KyProtoStatsProvider {
        KyProtoStatsProvider {
            conn: self.conn.clone(),
            protocol_stats: self.protocol_stats.clone(),
        }
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.conn.close(0, "KyProto connection closed");
    }
}

#[derive(Debug, Clone)]
pub struct KyProtoStatsProvider {
    conn: kynet::Connection,
    protocol_stats: KyArc<KyMutex<ProtocolStats>>,
}

impl KyProtoStatsProvider {
    pub async fn connection_stats(&self) -> ConnectionStats {
        self.conn.stats().await
    }

    pub fn protocol_stats(&self) -> ProtocolStats {
        self.protocol_stats.lock().clone()
    }
}
