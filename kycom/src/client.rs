// Project Kyber: client.rs
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

use crate::connection::Connection;
use crate::{ipc, serial, KyComAddr};

use async_trait::async_trait;
use kymux_types as types;
use kymux_types::ProtocolError;
#[allow(unused)]
use log::{debug, error, info, warn};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn ready(connection: &mut Connection) -> Result<(), ProtocolError> {
    let endpoint_id = connection.addr.endpoint_id;
    connection
        .write_u16(endpoint_id)
        .await
        .map_err(ProtocolError::new)?;
    connection.read_u8().await.map_err(ProtocolError::new)?;
    Ok(())
}

pub struct VideoClientEndpoint {
    connection: Connection,
}

impl VideoClientEndpoint {
    pub async fn connect(addr: KyComAddr) -> std::io::Result<Self> {
        let connection = Connection::connect(addr).await?;
        Ok(Self { connection })
    }
}

#[async_trait]
impl types::ProtocolEndpointDriver for VideoClientEndpoint {
    type Protocol = types::VideoClientProtocol;

    fn id(&self) -> u16 {
        self.connection.addr.endpoint_id
    }

    async fn ready_boxed(mut self: Box<Self>) -> Result<Self::Protocol, ProtocolError> {
        ready(&mut self.connection).await?;
        let recv = ipc::create_recv_protocol(self.connection, serial::av::AVPacketDeserializer);
        Ok(Self::Protocol { recv })
    }
}

pub struct VideoServerEndpoint {
    connection: Connection,
}

impl VideoServerEndpoint {
    pub async fn connect(addr: KyComAddr) -> std::io::Result<Self> {
        let connection = Connection::connect(addr).await?;
        Ok(Self { connection })
    }
}

#[async_trait]
impl types::ProtocolEndpointDriver for VideoServerEndpoint {
    type Protocol = types::VideoServerProtocol;

    fn id(&self) -> u16 {
        self.connection.addr.endpoint_id
    }

    async fn ready_boxed(mut self: Box<Self>) -> Result<Self::Protocol, ProtocolError> {
        ready(&mut self.connection).await?;
        let send = ipc::create_send_protocol(self.connection, serial::av::AVPacketSerializer);
        Ok(Self::Protocol { send })
    }
}

pub struct AudioClientEndpoint {
    connection: Connection,
}

impl AudioClientEndpoint {
    pub async fn connect(addr: KyComAddr) -> std::io::Result<Self> {
        let connection = Connection::connect(addr).await?;
        Ok(Self { connection })
    }
}

#[async_trait]
impl types::ProtocolEndpointDriver for AudioClientEndpoint {
    type Protocol = types::AudioClientProtocol;

    fn id(&self) -> u16 {
        self.connection.addr.endpoint_id
    }

    async fn ready_boxed(mut self: Box<Self>) -> Result<Self::Protocol, ProtocolError> {
        ready(&mut self.connection).await?;
        let recv = ipc::create_recv_protocol(self.connection, serial::av::AVPacketDeserializer);
        Ok(Self::Protocol { recv })
    }
}

pub struct AudioServerEndpoint {
    connection: Connection,
}

impl AudioServerEndpoint {
    pub async fn connect(addr: KyComAddr) -> std::io::Result<Self> {
        let connection = Connection::connect(addr).await?;
        Ok(Self { connection })
    }
}

#[async_trait]
impl types::ProtocolEndpointDriver for AudioServerEndpoint {
    type Protocol = types::AudioServerProtocol;

    fn id(&self) -> u16 {
        self.connection.addr.endpoint_id
    }

    async fn ready_boxed(mut self: Box<Self>) -> Result<Self::Protocol, ProtocolError> {
        ready(&mut self.connection).await?;
        let send = ipc::create_send_protocol(self.connection, serial::av::AVPacketSerializer);
        Ok(Self::Protocol { send })
    }
}

pub struct DataEndpoint {
    connection: Connection,
}

impl DataEndpoint {
    pub async fn connect(addr: KyComAddr) -> std::io::Result<Self> {
        let connection = Connection::connect(addr).await?;
        Ok(Self { connection })
    }
}

#[async_trait]
impl types::ProtocolEndpointDriver for DataEndpoint {
    type Protocol = types::DataProtocol;

    fn id(&self) -> u16 {
        self.connection.addr.endpoint_id
    }

    async fn ready_boxed(mut self: Box<Self>) -> Result<Self::Protocol, ProtocolError> {
        ready(&mut self.connection).await?;
        let (send, recv) = ipc::create_bi_protocol(
            self.connection.read,
            self.connection.write,
            serial::data::DataPacketSerializer,
            serial::data::DataPacketDeserializer,
        );
        Ok(Self::Protocol { send, recv })
    }
}

pub struct InputEndpoint {
    connection: Connection,
}

impl InputEndpoint {
    pub async fn connect(addr: KyComAddr) -> std::io::Result<Self> {
        let connection = Connection::connect(addr).await?;
        Ok(Self { connection })
    }
}

#[async_trait]
impl types::ProtocolEndpointDriver for InputEndpoint {
    type Protocol = types::InputProtocol;

    fn id(&self) -> u16 {
        self.connection.addr.endpoint_id
    }

    async fn ready_boxed(mut self: Box<Self>) -> Result<Self::Protocol, ProtocolError> {
        ready(&mut self.connection).await?;
        let (send, recv) = ipc::create_bi_protocol(
            self.connection.read,
            self.connection.write,
            serial::input::InputPacketSerializer,
            serial::input::InputPacketDeserializer,
        );
        Ok(Self::Protocol { send, recv })
    }
}

pub struct MetricsClientEndpoint {
    connection: Connection,
}

impl MetricsClientEndpoint {
    pub async fn connect(addr: KyComAddr) -> std::io::Result<Self> {
        let connection = Connection::connect(addr).await?;
        Ok(Self { connection })
    }
}

#[async_trait]
impl types::ProtocolEndpointDriver for MetricsClientEndpoint {
    type Protocol = types::MetricsClientProtocol;

    fn id(&self) -> u16 {
        self.connection.addr.endpoint_id
    }

    async fn ready_boxed(mut self: Box<Self>) -> Result<Self::Protocol, ProtocolError> {
        ready(&mut self.connection).await?;
        let recv =
            ipc::create_recv_protocol(self.connection, serial::metrics::MetricsPacketDeserializer);
        Ok(Self::Protocol { recv })
    }
}

pub struct MetricsServerEndpoint {
    connection: Connection,
}

impl MetricsServerEndpoint {
    pub async fn connect(addr: KyComAddr) -> std::io::Result<Self> {
        let connection = Connection::connect(addr).await?;
        Ok(Self { connection })
    }
}

#[async_trait]
impl types::ProtocolEndpointDriver for MetricsServerEndpoint {
    type Protocol = types::MetricsServerProtocol;

    fn id(&self) -> u16 {
        self.connection.addr.endpoint_id
    }

    async fn ready_boxed(mut self: Box<Self>) -> Result<Self::Protocol, ProtocolError> {
        ready(&mut self.connection).await?;
        let send =
            ipc::create_send_protocol(self.connection, serial::metrics::MetricsPacketSerializer);
        Ok(Self::Protocol { send })
    }
}
