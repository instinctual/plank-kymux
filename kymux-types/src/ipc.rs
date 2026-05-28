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

use crate::{ProtocolError, ProtocolRecv, ProtocolRecvDriver, ProtocolSend, ProtocolSendDriver};

use kymux_util::*;

use std::marker::PhantomData;

use crate::serial::Serializable;
use async_trait::async_trait;
use tokio::io::AsyncWriteExt;

struct IpcSend<W, P> {
    writer: W,
    _marker: PhantomData<P>,
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl<W: KyAsyncWrite + Unpin, P: Serializable + Send> ProtocolSendDriver for IpcSend<W, P> {
    type Packet = P;

    async fn send(&mut self, packet: P) -> Result<(), ProtocolError> {
        packet
            .write(&mut self.writer)
            .await
            .map_err(ProtocolError::new)?;
        self.writer.flush().await.map_err(ProtocolError::new)
    }
}

struct IpcRecv<R, P> {
    reader: R,
    _marker: PhantomData<P>,
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl<R: KyAsyncRead + Unpin, P: Serializable + Send> ProtocolRecvDriver for IpcRecv<R, P> {
    type Packet = P;

    async fn recv(&mut self) -> Result<Option<P>, ProtocolError> {
        P::read(&mut self.reader).await.map_err(ProtocolError::new)
    }
}

pub fn create_send_protocol<T>(writer: impl KyAsyncWrite + Unpin + 'static) -> ProtocolSend<T>
where
    T: Serializable + Send + 'static,
{
    ProtocolSend::new(IpcSend {
        writer,
        _marker: PhantomData,
    })
}

pub fn create_recv_protocol<T>(reader: impl KyAsyncRead + Unpin + 'static) -> ProtocolRecv<T>
where
    T: Serializable + Send + 'static,
{
    ProtocolRecv::new(IpcRecv {
        reader,
        _marker: PhantomData,
    })
}

pub fn create_bi_protocol<TX, RX>(
    reader: impl KyAsyncRead + Unpin + 'static,
    writer: impl KyAsyncWrite + Unpin + 'static,
) -> (ProtocolSend<TX>, ProtocolRecv<RX>)
where
    TX: Serializable + Send + 'static,
    RX: Serializable + Send + 'static,
{
    let send = create_send_protocol(writer);
    let recv = create_recv_protocol(reader);
    (send, recv)
}
