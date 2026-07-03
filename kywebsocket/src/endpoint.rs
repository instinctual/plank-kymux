// Project Kyber: endpoint.rs
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

use crate::ipc_msg;
use crate::Connection;

use async_trait::async_trait;
use kymux_types as types;
use kymux_types::ProtocolError;
use tokio::sync::oneshot;

pub struct Channel {
    rx: oneshot::Receiver<std::io::Result<Connection>>,
}

impl Channel {
    pub fn new(rx: oneshot::Receiver<std::io::Result<Connection>>) -> Self {
        Self { rx }
    }

    pub fn from_connection(conn: Connection) -> Self {
        let (tx, rx) = oneshot::channel();
        tx.send(Ok(conn)).expect("rx cannot be dropped");
        Self { rx }
    }

    async fn ready_internal(self) -> std::io::Result<Connection> {
        self.rx
            .await
            .map_err(|_| std::io::ErrorKind::ConnectionAborted)?
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

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl types::ProtocolEndpointDriver for VideoClientEndpointDriver {
    type Protocol = types::VideoClientProtocol;

    async fn ready_boxed(mut self: Box<Self>) -> Result<Self::Protocol, ProtocolError> {
        let conn = self.channel.ready().await?;
        let recv = ipc_msg::create_message_recv_protocol(conn.recv);
        Ok(Self::Protocol { recv })
    }
}

struct VideoServerEndpointDriver {
    channel: Channel,
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl types::ProtocolEndpointDriver for VideoServerEndpointDriver {
    type Protocol = types::VideoServerProtocol;

    async fn ready_boxed(mut self: Box<Self>) -> Result<Self::Protocol, ProtocolError> {
        let conn = self.channel.ready().await?;
        let send = ipc_msg::create_message_send_protocol(conn.send);
        Ok(Self::Protocol { send })
    }
}

struct AudioClientEndpointDriver {
    channel: Channel,
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl types::ProtocolEndpointDriver for AudioClientEndpointDriver {
    type Protocol = types::AudioClientProtocol;

    async fn ready_boxed(mut self: Box<Self>) -> Result<Self::Protocol, ProtocolError> {
        let conn = self.channel.ready().await?;
        let recv = ipc_msg::create_message_recv_protocol(conn.recv);
        Ok(Self::Protocol { recv })
    }
}

struct AudioServerEndpointDriver {
    channel: Channel,
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl types::ProtocolEndpointDriver for AudioServerEndpointDriver {
    type Protocol = types::AudioServerProtocol;

    async fn ready_boxed(mut self: Box<Self>) -> Result<Self::Protocol, ProtocolError> {
        let conn = self.channel.ready().await?;
        let send = ipc_msg::create_message_send_protocol(conn.send);
        Ok(Self::Protocol { send })
    }
}

struct DataEndpointDriver {
    channel: Channel,
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl types::ProtocolEndpointDriver for DataEndpointDriver {
    type Protocol = types::DataProtocol;

    async fn ready_boxed(mut self: Box<Self>) -> Result<Self::Protocol, ProtocolError> {
        let conn = self.channel.ready().await?;
        let (send, recv) = ipc_msg::create_message_bi_protocol(conn.send, conn.recv);
        Ok(Self::Protocol { send, recv })
    }
}

struct InputEndpointDriver {
    channel: Channel,
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl types::ProtocolEndpointDriver for InputEndpointDriver {
    type Protocol = types::InputProtocol;

    async fn ready_boxed(mut self: Box<Self>) -> Result<Self::Protocol, ProtocolError> {
        let conn = self.channel.ready().await?;
        let (send, recv) = ipc_msg::create_message_bi_protocol(conn.send, conn.recv);
        Ok(Self::Protocol { send, recv })
    }
}

struct MetricsClientEndpointDriver {
    channel: Channel,
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl types::ProtocolEndpointDriver for MetricsClientEndpointDriver {
    type Protocol = types::MetricsClientProtocol;

    async fn ready_boxed(mut self: Box<Self>) -> Result<Self::Protocol, ProtocolError> {
        let conn = self.channel.ready().await?;
        let recv = ipc_msg::create_message_recv_protocol(conn.recv);
        Ok(Self::Protocol { recv })
    }
}

struct MetricsServerEndpointDriver {
    channel: Channel,
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl types::ProtocolEndpointDriver for MetricsServerEndpointDriver {
    type Protocol = types::MetricsServerProtocol;

    async fn ready_boxed(mut self: Box<Self>) -> Result<Self::Protocol, ProtocolError> {
        let conn = self.channel.ready().await?;
        let send = ipc_msg::create_message_send_protocol(conn.send);
        Ok(Self::Protocol { send })
    }
}
