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

use crate::{ipc, serial, KyComAddr};

use async_trait::async_trait;
use kymux_types as types;
use kymux_types::ProtocolError;
#[allow(unused)]
use log::{debug, error, info, warn};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

async fn ready(tcp_stream: &mut TcpStream, endpoint_id: u16) -> Result<(), ProtocolError> {
    tcp_stream
        .write_u16(endpoint_id)
        .await
        .map_err(ProtocolError::new)?;
    tcp_stream.read_u8().await.map_err(ProtocolError::new)?;
    Ok(())
}

pub struct VideoClientEndpoint {
    addr: KyComAddr,
    tcp_stream: TcpStream,
}

impl VideoClientEndpoint {
    pub async fn connect(addr: KyComAddr) -> std::io::Result<Self> {
        let tcp_stream = TcpStream::connect(addr.addr).await?;
        Ok(Self { addr, tcp_stream })
    }
}

#[async_trait]
impl types::ProtocolEndpointDriver for VideoClientEndpoint {
    type Protocol = types::VideoClientProtocol;

    fn id(&self) -> u16 {
        self.addr.endpoint_id
    }

    async fn ready_boxed(mut self: Box<Self>) -> Result<Self::Protocol, ProtocolError> {
        ready(&mut self.tcp_stream, self.addr.endpoint_id).await?;
        let recv = ipc::create_recv_protocol(self.tcp_stream, serial::av::AVPacketDeserializer);
        Ok(Self::Protocol { recv })
    }
}

pub struct VideoServerEndpoint {
    addr: KyComAddr,
    tcp_stream: TcpStream,
}

impl VideoServerEndpoint {
    pub async fn connect(addr: KyComAddr) -> std::io::Result<Self> {
        let tcp_stream = TcpStream::connect(addr.addr).await?;
        Ok(Self { addr, tcp_stream })
    }
}

#[async_trait]
impl types::ProtocolEndpointDriver for VideoServerEndpoint {
    type Protocol = types::VideoServerProtocol;

    fn id(&self) -> u16 {
        self.addr.endpoint_id
    }

    async fn ready_boxed(mut self: Box<Self>) -> Result<Self::Protocol, ProtocolError> {
        ready(&mut self.tcp_stream, self.addr.endpoint_id).await?;
        let send = ipc::create_send_protocol(self.tcp_stream, serial::av::AVPacketSerializer);
        Ok(Self::Protocol { send })
    }
}

pub struct DataEndpoint {
    addr: KyComAddr,
    tcp_stream: TcpStream,
}

impl DataEndpoint {
    pub async fn connect(addr: KyComAddr) -> std::io::Result<Self> {
        let tcp_stream = TcpStream::connect(addr.addr).await?;
        Ok(Self { addr, tcp_stream })
    }
}

#[async_trait]
impl types::ProtocolEndpointDriver for DataEndpoint {
    type Protocol = types::DataProtocol;

    fn id(&self) -> u16 {
        self.addr.endpoint_id
    }

    async fn ready_boxed(mut self: Box<Self>) -> Result<Self::Protocol, ProtocolError> {
        ready(&mut self.tcp_stream, self.addr.endpoint_id).await?;
        let (send, recv) = ipc::create_bi_protocol(
            self.tcp_stream,
            serial::data::DataPacketSerializer,
            serial::data::DataPacketDeserializer,
        );
        Ok(Self::Protocol { send, recv })
    }
}

pub struct InputEndpoint {
    addr: KyComAddr,
    tcp_stream: TcpStream,
}

impl InputEndpoint {
    pub async fn connect(addr: KyComAddr) -> std::io::Result<Self> {
        let tcp_stream = TcpStream::connect(addr.addr).await?;
        Ok(Self { addr, tcp_stream })
    }
}

#[async_trait]
impl types::ProtocolEndpointDriver for InputEndpoint {
    type Protocol = types::InputProtocol;

    fn id(&self) -> u16 {
        self.addr.endpoint_id
    }

    async fn ready_boxed(mut self: Box<Self>) -> Result<Self::Protocol, ProtocolError> {
        ready(&mut self.tcp_stream, self.addr.endpoint_id).await?;
        let (send, recv) = ipc::create_bi_protocol(
            self.tcp_stream,
            serial::input::InputPacketSerializer,
            serial::input::InputPacketDeserializer,
        );
        Ok(Self::Protocol { send, recv })
    }
}

pub struct MetricsClientEndpoint {
    addr: KyComAddr,
    tcp_stream: TcpStream,
}

impl MetricsClientEndpoint {
    pub async fn connect(addr: KyComAddr) -> std::io::Result<Self> {
        let tcp_stream = TcpStream::connect(addr.addr).await?;
        Ok(Self { addr, tcp_stream })
    }
}

#[async_trait]
impl types::ProtocolEndpointDriver for MetricsClientEndpoint {
    type Protocol = types::MetricsClientProtocol;

    fn id(&self) -> u16 {
        self.addr.endpoint_id
    }

    async fn ready_boxed(mut self: Box<Self>) -> Result<Self::Protocol, ProtocolError> {
        ready(&mut self.tcp_stream, self.addr.endpoint_id).await?;
        let recv =
            ipc::create_recv_protocol(self.tcp_stream, serial::metrics::MetricsPacketDeserializer);
        Ok(Self::Protocol { recv })
    }
}

pub struct MetricsServerEndpoint {
    addr: KyComAddr,
    tcp_stream: TcpStream,
}

impl MetricsServerEndpoint {
    pub async fn connect(addr: KyComAddr) -> std::io::Result<Self> {
        let tcp_stream = TcpStream::connect(addr.addr).await?;
        Ok(Self { addr, tcp_stream })
    }
}

#[async_trait]
impl types::ProtocolEndpointDriver for MetricsServerEndpoint {
    type Protocol = types::MetricsServerProtocol;

    fn id(&self) -> u16 {
        self.addr.endpoint_id
    }

    async fn ready_boxed(mut self: Box<Self>) -> Result<Self::Protocol, ProtocolError> {
        ready(&mut self.tcp_stream, self.addr.endpoint_id).await?;
        let send =
            ipc::create_send_protocol(self.tcp_stream, serial::metrics::MetricsPacketSerializer);
        Ok(Self::Protocol { send })
    }
}
