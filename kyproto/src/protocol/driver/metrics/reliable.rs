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

use crate::protocol::{ProtocolError, ProtocolRecvDriver, ProtocolSendDriver};
use crate::router::{KyChannel, KyChannelRecv, KyChannelSend};

use async_trait::async_trait;
use bytes::BytesMut;
use kymux_types::metrics::*;
use kynet::error::ReadExactError;
use kynet::{RecvStream, SendStream};

pub(crate) struct ReliableProtocolSendDriver {
    ky_channel: KyChannel,
    send: SendStream,
}

impl ReliableProtocolSendDriver {
    pub(crate) async fn start(ky_channel: KyChannel) -> Result<Self, ProtocolError> {
        let send = ky_channel.open_uni().await.map_err(ProtocolError::new)?;
        Ok(Self { ky_channel, send })
    }
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ProtocolSendDriver for ReliableProtocolSendDriver {
    type Packet = MetricsPacket;

    async fn send(&mut self, packet: MetricsPacket) -> Result<(), ProtocolError> {
        let size =
            u16::try_from(packet.payload.len()).expect("Input packet size must fit in 16 bits");

        self.send
            .write_all(&size.to_be_bytes())
            .await
            .map_err(ProtocolError::new)?;
        self.send
            .write_all(&packet.payload)
            .await
            .map_err(ProtocolError::new)?;

        Ok(())
    }
}

pub(crate) struct ReliableProtocolRecvDriver {
    ky_channel: KyChannel,
    recv: RecvStream,
}

impl ReliableProtocolRecvDriver {
    pub(crate) async fn start(mut ky_channel: KyChannel) -> Result<Self, ProtocolError> {
        let recv = ky_channel.accept_uni().await.map_err(ProtocolError::new)?;
        Ok(Self { ky_channel, recv })
    }
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ProtocolRecvDriver for ReliableProtocolRecvDriver {
    type Packet = MetricsPacket;

    async fn recv(&mut self) -> Result<Option<MetricsPacket>, ProtocolError> {
        let mut buf = [0; 2];
        self.recv
            .read_exact(&mut buf)
            .await
            .map_err(ProtocolError::new)?;
        let size = u16::from_be_bytes(buf);

        let mut buf = BytesMut::zeroed(size as usize);
        self.recv
            .read_exact(&mut buf)
            .await
            .map_err(ProtocolError::new)?;
        let payload = buf.freeze();

        let packet = MetricsPacket { payload };

        Ok(Some(packet))
    }
}
