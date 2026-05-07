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
use async_trait::async_trait;
use kymux_types::*;
use tokio::io::{AsyncRead, AsyncWrite};

struct IpcSend<T> {
    writer: Box<dyn AsyncWrite + Send + Unpin>,
    serializer: Box<dyn Serializer<Packet = T> + Send>,
}

impl<T> IpcSend<T> {
    pub(crate) fn new(
        writer: impl AsyncWrite + Send + Unpin + 'static,
        serializer: impl Serializer<Packet = T> + Send + 'static,
    ) -> Self {
        Self {
            writer: Box::new(writer),
            serializer: Box::new(serializer),
        }
    }
}

#[async_trait]
impl<T: Send + 'static> ProtocolSendDriver for IpcSend<T> {
    type Packet = T;

    async fn send(&mut self, packet: T) -> Result<(), ProtocolError> {
        self.serializer
            .write(packet, &mut self.writer)
            .await
            .map_err(ProtocolError::new)
    }
}

struct IpcRecv<T> {
    reader: Box<dyn AsyncRead + Send + Unpin>,
    deserializer: Box<dyn Deserializer<Packet = T> + Send>,
}

impl<T> IpcRecv<T> {
    pub(crate) fn new(
        reader: impl AsyncRead + Send + Unpin + 'static,
        deserializer: impl Deserializer<Packet = T> + Send + 'static,
    ) -> Self {
        Self {
            reader: Box::new(reader),
            deserializer: Box::new(deserializer),
        }
    }
}

#[async_trait]
impl<T: Send + 'static> ProtocolRecvDriver for IpcRecv<T> {
    type Packet = T;

    async fn recv(&mut self) -> Result<Option<T>, ProtocolError> {
        self.deserializer
            .read(&mut self.reader)
            .await
            .map_err(ProtocolError::new)
    }
}

pub fn create_send_protocol<T>(
    writer: impl AsyncWrite + Send + Unpin + 'static,
    serializer: impl Serializer<Packet = T> + Send + 'static,
) -> ProtocolSend<T>
where
    T: Send + 'static,
{
    let driver = IpcSend::new(writer, serializer);
    ProtocolSend::new(driver)
}

pub fn create_recv_protocol<T>(
    reader: impl AsyncRead + Send + Unpin + 'static,
    deserializer: impl Deserializer<Packet = T> + Send + 'static,
) -> ProtocolRecv<T>
where
    T: Send + 'static,
{
    let driver = IpcRecv::new(reader, deserializer);
    ProtocolRecv::new(driver)
}

pub fn create_bi_protocol<TX, RX>(
    reader: impl AsyncRead + Send + Unpin + 'static,
    writer: impl AsyncWrite + Send + Unpin + 'static,
    serializer: impl Serializer<Packet = TX> + Send + 'static,
    deserializer: impl Deserializer<Packet = RX> + Send + 'static,
) -> (ProtocolSend<TX>, ProtocolRecv<RX>)
where
    TX: Send + 'static,
    RX: Send + 'static,
{
    let send = create_send_protocol(writer, serializer);
    let recv = create_recv_protocol(reader, deserializer);
    (send, recv)
}
