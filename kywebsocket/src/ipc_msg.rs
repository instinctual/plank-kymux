// Project Kyber: ipc_msg.rs
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

// like kymux_types::ipc, but based on websocket messages instead of streams

use crate::{MessageRecvStream, MessageSendStream};

use kymux_types as types;
use kymux_types::{FramedSerializable, ProtocolError, ProtocolRecv, ProtocolSend};
use kymux_util::*;

use std::marker::PhantomData;

use async_trait::async_trait;
use bytes::Bytes;

struct IpcMessageSend<P> {
    send: MessageSendStream,
    _marker: PhantomData<P>,
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl<P: FramedSerializable + KySend> types::ProtocolSendDriver for IpcMessageSend<P> {
    type Packet = P;

    async fn send(&mut self, packet: Self::Packet) -> Result<(), ProtocolError> {
        let msg = Bytes::from(packet.serialize());
        self.send.send(msg).await.map_err(ProtocolError::new)
    }
}

struct IpcMessageRecv<P> {
    recv: MessageRecvStream,
    _marker: PhantomData<P>,
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl<P: FramedSerializable + KySend> types::ProtocolRecvDriver for IpcMessageRecv<P> {
    type Packet = P;

    async fn recv(&mut self) -> Result<Option<Self::Packet>, ProtocolError> {
        let Some(msg) = self.recv.recv().await.map_err(ProtocolError::new)? else {
            return Ok(None);
        };

        let packet = P::parse(&msg).map_err(ProtocolError::new)?;
        Ok(Some(packet))
    }
}

pub fn create_message_send_protocol<T>(send: MessageSendStream) -> types::ProtocolSend<T>
where
    T: FramedSerializable + KySend + 'static,
{
    ProtocolSend::new(IpcMessageSend {
        send,
        _marker: PhantomData,
    })
}

pub fn create_message_recv_protocol<T>(recv: MessageRecvStream) -> types::ProtocolRecv<T>
where
    T: FramedSerializable + KySend + 'static,
{
    ProtocolRecv::new(IpcMessageRecv {
        recv,
        _marker: PhantomData,
    })
}

pub fn create_message_bi_protocol<TX, RX>(
    send: MessageSendStream,
    recv: MessageRecvStream,
) -> (types::ProtocolSend<TX>, types::ProtocolRecv<RX>)
where
    TX: FramedSerializable + KySend + 'static,
    RX: FramedSerializable + KySend + 'static,
{
    let send = create_message_send_protocol(send);
    let recv = create_message_recv_protocol(recv);
    (send, recv)
}
