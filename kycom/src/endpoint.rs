// Project Kyber: client.rs
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

use crate::KyComAddr;
use crate::connection::Connection;

use async_trait::async_trait;
use kymux_types as types;
use kymux_types::{ProtocolError, ipc};
#[allow(unused)]
use log::{debug, error, info, warn};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::oneshot;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) enum ChannelRole {
    Client,
    Server,
}

pub struct Channel {
    addr: KyComAddr,
    rx: oneshot::Receiver<std::io::Result<Connection>>,
    role: ChannelRole,
}

impl Channel {
    pub(crate) fn new(
        addr: KyComAddr,
        rx: oneshot::Receiver<std::io::Result<Connection>>,
        role: ChannelRole,
    ) -> Self {
        Self { addr, rx, role }
    }

    pub fn addr(&self) -> KyComAddr {
        self.addr
    }

    pub fn connect(addr: KyComAddr) -> Self {
        let (tx, rx) = oneshot::channel();
        tokio::spawn(async move {
            let result_connection = Connection::connect(addr).await;
            let _ = tx.send(result_connection);
        });

        Self::new(addr, rx, ChannelRole::Client)
    }

    async fn ready_internal(self) -> std::io::Result<Connection> {
        let endpoint_id = self.addr.endpoint_id;
        let connection = self
            .rx
            .await
            .map_err(|_| std::io::ErrorKind::ConnectionAborted)?;
        let mut connection = connection?;

        assert!(connection.addr.endpoint_id == endpoint_id);
        if self.role == ChannelRole::Client {
            connection.write_u16(endpoint_id).await?;
            connection.flush().await?;
            connection.read_u8().await?;
        } else {
            connection.write_all(&[0]).await?;
            connection.flush().await?;
        }

        Ok(connection)
    }

    async fn ready(self) -> Result<Connection, ProtocolError> {
        self.ready_internal().await.map_err(ProtocolError::new)
    }

    pub fn into_video_client_endpoint(self) -> types::VideoClientEndpoint {
        VideoClientEndpointDriver { channel: self }.into()
    }

    pub fn into_video_server_endpoint(self) -> types::VideoServerEndpoint {
        VideoServerEndpointDriver { channel: self }.into()
    }

    pub fn into_audio_client_endpoint(self) -> types::AudioClientEndpoint {
        AudioClientEndpointDriver { channel: self }.into()
    }

    pub fn into_audio_server_endpoint(self) -> types::AudioServerEndpoint {
        AudioServerEndpointDriver { channel: self }.into()
    }

    pub fn into_data_endpoint(self) -> types::DataEndpoint {
        DataEndpointDriver { channel: self }.into()
    }

    pub fn into_input_endpoint(self) -> types::InputEndpoint {
        InputEndpointDriver { channel: self }.into()
    }

    pub fn into_metrics_client_endpoint(self) -> types::MetricsClientEndpoint {
        MetricsClientEndpointDriver { channel: self }.into()
    }

    pub fn into_metrics_server_endpoint(self) -> types::MetricsServerEndpoint {
        MetricsServerEndpointDriver { channel: self }.into()
    }
}

struct VideoClientEndpointDriver {
    channel: Channel,
}

#[async_trait]
impl types::ProtocolEndpointDriver for VideoClientEndpointDriver {
    type Protocol = types::VideoClientProtocol;

    async fn ready_boxed(mut self: Box<Self>) -> Result<Self::Protocol, ProtocolError> {
        let connection = self.channel.ready().await?;
        let recv = ipc::create_recv_protocol(connection);
        Ok(Self::Protocol { recv })
    }
}

struct VideoServerEndpointDriver {
    channel: Channel,
}

#[async_trait]
impl types::ProtocolEndpointDriver for VideoServerEndpointDriver {
    type Protocol = types::VideoServerProtocol;

    async fn ready_boxed(mut self: Box<Self>) -> Result<Self::Protocol, ProtocolError> {
        let connection = self.channel.ready().await?;
        let send = ipc::create_send_protocol(connection);
        Ok(Self::Protocol { send })
    }
}

struct AudioClientEndpointDriver {
    channel: Channel,
}

#[async_trait]
impl types::ProtocolEndpointDriver for AudioClientEndpointDriver {
    type Protocol = types::AudioClientProtocol;

    async fn ready_boxed(mut self: Box<Self>) -> Result<Self::Protocol, ProtocolError> {
        let connection = self.channel.ready().await?;
        let recv = ipc::create_recv_protocol(connection);
        Ok(Self::Protocol { recv })
    }
}

struct AudioServerEndpointDriver {
    channel: Channel,
}

#[async_trait]
impl types::ProtocolEndpointDriver for AudioServerEndpointDriver {
    type Protocol = types::AudioServerProtocol;

    async fn ready_boxed(mut self: Box<Self>) -> Result<Self::Protocol, ProtocolError> {
        let connection = self.channel.ready().await?;
        let send = ipc::create_send_protocol(connection);
        Ok(Self::Protocol { send })
    }
}

struct DataEndpointDriver {
    channel: Channel,
}

#[async_trait]
impl types::ProtocolEndpointDriver for DataEndpointDriver {
    type Protocol = types::DataProtocol;

    async fn ready_boxed(mut self: Box<Self>) -> Result<Self::Protocol, ProtocolError> {
        let connection = self.channel.ready().await?;
        let (send, recv) = ipc::create_bi_protocol(connection.read, connection.write);
        Ok(Self::Protocol { send, recv })
    }
}

struct InputEndpointDriver {
    channel: Channel,
}

#[async_trait]
impl types::ProtocolEndpointDriver for InputEndpointDriver {
    type Protocol = types::InputProtocol;

    async fn ready_boxed(mut self: Box<Self>) -> Result<Self::Protocol, ProtocolError> {
        let connection = self.channel.ready().await?;
        let (send, recv) = ipc::create_bi_protocol(connection.read, connection.write);
        Ok(Self::Protocol { send, recv })
    }
}

struct MetricsClientEndpointDriver {
    channel: Channel,
}

#[async_trait]
impl types::ProtocolEndpointDriver for MetricsClientEndpointDriver {
    type Protocol = types::MetricsClientProtocol;

    async fn ready_boxed(mut self: Box<Self>) -> Result<Self::Protocol, ProtocolError> {
        let connection = self.channel.ready().await?;
        let recv = ipc::create_recv_protocol(connection);
        Ok(Self::Protocol { recv })
    }
}

struct MetricsServerEndpointDriver {
    channel: Channel,
}

#[async_trait]
impl types::ProtocolEndpointDriver for MetricsServerEndpointDriver {
    type Protocol = types::MetricsServerProtocol;

    async fn ready_boxed(mut self: Box<Self>) -> Result<Self::Protocol, ProtocolError> {
        let connection = self.channel.ready().await?;
        let send = ipc::create_send_protocol(connection);
        Ok(Self::Protocol { send })
    }
}
