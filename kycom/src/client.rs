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
use kymux_types::*;
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
impl ProtocolEndpoint for VideoClientEndpoint {
    type Protocol = ProtocolRecv<AVPacket>;

    fn id(&self) -> u16 {
        self.addr.endpoint_id
    }

    async fn ready_boxed(mut self: Box<Self>) -> Result<Self::Protocol, ProtocolError> {
        ready(&mut self.tcp_stream, self.addr.endpoint_id).await?;
        let ipc = ipc::create_recv_protocol(self.tcp_stream, serial::av::AVPacketDeserializer);
        Ok(ipc)
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
impl ProtocolEndpoint for VideoServerEndpoint {
    type Protocol = ProtocolSend<AVPacket>;

    fn id(&self) -> u16 {
        self.addr.endpoint_id
    }

    async fn ready_boxed(mut self: Box<Self>) -> Result<Self::Protocol, ProtocolError> {
        ready(&mut self.tcp_stream, self.addr.endpoint_id).await?;
        let ipc = ipc::create_send_protocol(self.tcp_stream, serial::av::AVPacketSerializer);
        Ok(ipc)
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
impl ProtocolEndpoint for DataEndpoint {
    type Protocol = (ProtocolSend<DataPacket>, ProtocolRecv<DataPacket>);

    fn id(&self) -> u16 {
        self.addr.endpoint_id
    }

    async fn ready_boxed(mut self: Box<Self>) -> Result<Self::Protocol, ProtocolError> {
        ready(&mut self.tcp_stream, self.addr.endpoint_id).await?;
        let ipc = ipc::create_bi_protocol(
            self.tcp_stream,
            serial::data::DataPacketSerializer,
            serial::data::DataPacketDeserializer,
        );
        Ok(ipc)
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
impl ProtocolEndpoint for InputEndpoint {
    type Protocol = (ProtocolSend<InputPacket>, ProtocolRecv<InputPacket>);

    fn id(&self) -> u16 {
        self.addr.endpoint_id
    }

    async fn ready_boxed(mut self: Box<Self>) -> Result<Self::Protocol, ProtocolError> {
        ready(&mut self.tcp_stream, self.addr.endpoint_id).await?;
        let ipc = ipc::create_bi_protocol(
            self.tcp_stream,
            serial::input::InputPacketSerializer,
            serial::input::InputPacketDeserializer,
        );
        Ok(ipc)
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
impl ProtocolEndpoint for MetricsClientEndpoint {
    type Protocol = ProtocolRecv<MetricsPacket>;

    fn id(&self) -> u16 {
        self.addr.endpoint_id
    }

    async fn ready_boxed(mut self: Box<Self>) -> Result<Self::Protocol, ProtocolError> {
        ready(&mut self.tcp_stream, self.addr.endpoint_id).await?;
        let ipc =
            ipc::create_recv_protocol(self.tcp_stream, serial::metrics::MetricsPacketDeserializer);
        Ok(ipc)
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
impl ProtocolEndpoint for MetricsServerEndpoint {
    type Protocol = ProtocolSend<MetricsPacket>;

    fn id(&self) -> u16 {
        self.addr.endpoint_id
    }

    async fn ready_boxed(mut self: Box<Self>) -> Result<Self::Protocol, ProtocolError> {
        ready(&mut self.tcp_stream, self.addr.endpoint_id).await?;
        let ipc =
            ipc::create_send_protocol(self.tcp_stream, serial::metrics::MetricsPacketSerializer);
        Ok(ipc)
    }
}
