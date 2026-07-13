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

    async fn ready(self) -> std::io::Result<Connection> {
        self.rx
            .await
            .map_err(|_| std::io::ErrorKind::ConnectionAborted)?
    }

    #[allow(private_bounds)]
    pub fn into_endpoint<P>(self) -> types::ProtocolEndpoint<P>
    where
        P: 'static,
        EndpointDriver<P>: types::ProtocolEndpointDriver<Protocol = P>,
    {
        types::ProtocolEndpoint::new(EndpointDriver {
            channel: self,
            _typ: std::marker::PhantomData,
        })
    }
}

struct EndpointDriver<P> {
    channel: Channel,
    _typ: std::marker::PhantomData<P>,
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl<T: types::FramedSerializable> types::ProtocolEndpointDriver
    for EndpointDriver<types::ProtocolRecv<T>>
{
    type Protocol = types::ProtocolRecv<T>;

    async fn ready_boxed(mut self: Box<Self>) -> Result<Self::Protocol, ProtocolError> {
        let connection = self.channel.ready().await.map_err(ProtocolError::new)?;
        Ok(ipc_msg::create_message_recv_protocol(connection.recv))
    }
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl<T: types::FramedSerializable> types::ProtocolEndpointDriver
    for EndpointDriver<types::ProtocolSend<T>>
{
    type Protocol = types::ProtocolSend<T>;

    async fn ready_boxed(mut self: Box<Self>) -> Result<Self::Protocol, ProtocolError> {
        let connection = self.channel.ready().await.map_err(ProtocolError::new)?;
        Ok(ipc_msg::create_message_send_protocol(connection.send))
    }
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl<T: types::FramedSerializable> types::ProtocolEndpointDriver
    for EndpointDriver<(types::ProtocolSend<T>, types::ProtocolRecv<T>)>
{
    type Protocol = (types::ProtocolSend<T>, types::ProtocolRecv<T>);

    async fn ready_boxed(mut self: Box<Self>) -> Result<Self::Protocol, ProtocolError> {
        let connection = self.channel.ready().await.map_err(ProtocolError::new)?;
        Ok(ipc_msg::create_message_bi_protocol(
            connection.send,
            connection.recv,
        ))
    }
}
