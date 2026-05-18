// Project Kyber: ipc.rs
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

use crate::serial::{Deserializer, Serializer};
use crate::{ProtocolError, ProtocolRecv, ProtocolRecvDriver, ProtocolSend, ProtocolSendDriver};

use kymux_util::*;

use async_trait::async_trait;
use tokio::io::AsyncWriteExt;

// These traits are required because KySend is not an auto-trait.
pub trait KySerializer: Serializer + KySend {}
impl<T: Serializer + KySend> KySerializer for T {}

pub trait KyDeserializer: Deserializer + KySend {}
impl<T: Deserializer + KySend> KyDeserializer for T {}

struct IpcSend<T> {
    writer: Box<dyn KyAsyncWrite + Unpin>,
    serializer: Box<dyn KySerializer<Packet = T>>,
}

impl<T> IpcSend<T> {
    pub(crate) fn new(
        writer: impl KyAsyncWrite + Unpin + 'static,
        serializer: impl KySerializer<Packet = T> + 'static,
    ) -> Self {
        Self {
            writer: Box::new(writer),
            serializer: Box::new(serializer),
        }
    }
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl<T: KySend + 'static> ProtocolSendDriver for IpcSend<T> {
    type Packet = T;

    async fn send(&mut self, packet: T) -> Result<(), ProtocolError> {
        self.serializer
            .write(packet, &mut self.writer)
            .await
            .map_err(ProtocolError::new)?;
        self.writer.flush().await.map_err(ProtocolError::new)
    }
}

struct IpcRecv<T> {
    reader: Box<dyn KyAsyncRead + Unpin>,
    deserializer: Box<dyn KyDeserializer<Packet = T>>,
}

impl<T> IpcRecv<T> {
    pub(crate) fn new(
        reader: impl KyAsyncRead + Unpin + 'static,
        deserializer: impl KyDeserializer<Packet = T> + 'static,
    ) -> Self {
        Self {
            reader: Box::new(reader),
            deserializer: Box::new(deserializer),
        }
    }
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl<T: KySend + 'static> ProtocolRecvDriver for IpcRecv<T> {
    type Packet = T;

    async fn recv(&mut self) -> Result<Option<T>, ProtocolError> {
        self.deserializer
            .read(&mut self.reader)
            .await
            .map_err(ProtocolError::new)
    }
}

pub fn create_send_protocol<T>(
    writer: impl KyAsyncWrite + Unpin + 'static,
    serializer: impl KySerializer<Packet = T> + 'static,
) -> ProtocolSend<T>
where
    T: KySend + 'static,
{
    let driver = IpcSend::new(writer, serializer);
    ProtocolSend::new(driver)
}

pub fn create_recv_protocol<T>(
    reader: impl KyAsyncRead + Unpin + 'static,
    deserializer: impl KyDeserializer<Packet = T> + 'static,
) -> ProtocolRecv<T>
where
    T: KySend + 'static,
{
    let driver = IpcRecv::new(reader, deserializer);
    ProtocolRecv::new(driver)
}

pub fn create_bi_protocol<TX, RX>(
    reader: impl KyAsyncRead + Unpin + 'static,
    writer: impl KyAsyncWrite + Unpin + 'static,
    serializer: impl KySerializer<Packet = TX> + 'static,
    deserializer: impl KyDeserializer<Packet = RX> + 'static,
) -> (ProtocolSend<TX>, ProtocolRecv<RX>)
where
    TX: KySend + 'static,
    RX: KySend + 'static,
{
    let send = create_send_protocol(writer, serializer);
    let recv = create_recv_protocol(reader, deserializer);
    (send, recv)
}
