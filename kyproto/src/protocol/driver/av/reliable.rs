// Project Kyber: reliable.rs
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

use crate::ProtocolStats;
use crate::protocol::driver;
use crate::protocol::{ProtocolError, ProtocolRecvDriver, ProtocolSendDriver};
use crate::router::KyChannel;

use async_trait::async_trait;
use bytes::BytesMut;
use kymux_types::av::*;
use kymux_util::*;
use kynet::{RecvStream, SendStream};
use tokio::io::AsyncWriteExt;

pub(crate) struct ReliableProtocolSendDriver {
    ky_channel: KyChannel,
    send: SendStream,
}

impl ReliableProtocolSendDriver {
    pub(crate) async fn start(
        ky_channel: KyChannel,
        _protocol_stats: &KyArc<KyMutex<ProtocolStats>>,
    ) -> Result<Self, ProtocolError> {
        let send = ky_channel.open_uni().await.map_err(ProtocolError::new)?;
        Ok(Self { ky_channel, send })
    }
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ProtocolSendDriver for ReliableProtocolSendDriver {
    type Packet = AVPacket;

    async fn send(&mut self, packet: AVPacket) -> Result<(), ProtocolError> {
        driver::write_packet(&mut self.send, &mut AVPacketSerializer, packet).await
    }
}

pub(crate) struct ReliableProtocolRecvDriver {
    ky_channel: KyChannel,
    recv: RecvStream,
}

impl ReliableProtocolRecvDriver {
    pub(crate) async fn start(
        mut ky_channel: KyChannel,
        _protocol_stats: &KyArc<KyMutex<ProtocolStats>>,
    ) -> Result<Self, ProtocolError> {
        let recv = ky_channel.accept_uni().await.map_err(ProtocolError::new)?;
        Ok(Self { ky_channel, recv })
    }
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ProtocolRecvDriver for ReliableProtocolRecvDriver {
    type Packet = AVPacket;

    async fn recv(&mut self) -> Result<Option<AVPacket>, ProtocolError> {
        driver::read_packet(&mut self.recv, &mut AVPacketDeserializer).await
    }
}
