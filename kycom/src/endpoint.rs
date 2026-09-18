// Project Kyber: client.rs
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

use crate::KyComAddr;
use crate::connection::Connection;

use async_trait::async_trait;
use kymux_types as types;
use kymux_types::{ProtocolError, ipc};
#[allow(unused)]
use log::{debug, error, info, warn};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::oneshot;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) enum ChannelRole {
    Client,
    Server,
}

pub struct Channel {
    addr: KyComAddr,
    rx: oneshot::Receiver<std::io::Result<Connection>>,
    role: ChannelRole,
}

impl Channel {
    pub(crate) fn new(
        addr: KyComAddr,
        rx: oneshot::Receiver<std::io::Result<Connection>>,
        role: ChannelRole,
    ) -> Self {
        Self { addr, rx, role }
    }

    pub fn addr(&self) -> KyComAddr {
        self.addr
    }

    pub fn connect(addr: KyComAddr) -> Self {
        let (tx, rx) = oneshot::channel();
        tokio::spawn(async move {
            let result_connection = Connection::connect(addr).await;
            let _ = tx.send(result_connection);
        });

        Self::new(addr, rx, ChannelRole::Client)
    }

    async fn ready(self) -> std::io::Result<Connection> {
        let endpoint_id = self.addr.endpoint_id;
        let connection = self
            .rx
            .await
            .map_err(|_| std::io::ErrorKind::ConnectionAborted)?;
        let mut connection = connection?;

        assert!(connection.addr.endpoint_id == endpoint_id);
        if self.role == ChannelRole::Client {
            connection.write_u16(endpoint_id).await?;
            connection.flush().await?;
            connection.read_u8().await?;
        } else {
            connection.write_all(&[0]).await?;
            connection.flush().await?;
        }

        Ok(connection)
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

#[async_trait]
impl<T: types::Serializable> types::ProtocolEndpointDriver
    for EndpointDriver<types::ProtocolRecv<T>>
{
    type Protocol = types::ProtocolRecv<T>;

    async fn ready_boxed(mut self: Box<Self>) -> Result<Self::Protocol, ProtocolError> {
        let connection = self.channel.ready().await.map_err(ProtocolError::new)?;
        // Keep both halves: Windows doesn't handle half-close properly (it resets the
        // connection after ~2 minutes of silence), so we can't drop the unused write half.
        Ok(ipc::create_recv_protocol(connection))
    }
}

#[async_trait]
impl<T: types::Serializable> types::ProtocolEndpointDriver
    for EndpointDriver<types::ProtocolSend<T>>
{
    type Protocol = types::ProtocolSend<T>;

    async fn ready_boxed(mut self: Box<Self>) -> Result<Self::Protocol, ProtocolError> {
        let connection = self.channel.ready().await.map_err(ProtocolError::new)?;
        // Keep both halves (see recv impl above) rather than dropping the unused read half.
        Ok(ipc::create_send_protocol(connection))
    }
}

#[async_trait]
impl<T: types::Serializable> types::ProtocolEndpointDriver
    for EndpointDriver<(types::ProtocolSend<T>, types::ProtocolRecv<T>)>
{
    type Protocol = (types::ProtocolSend<T>, types::ProtocolRecv<T>);

    async fn ready_boxed(mut self: Box<Self>) -> Result<Self::Protocol, ProtocolError> {
        let connection = self.channel.ready().await.map_err(ProtocolError::new)?;
        Ok(ipc::create_bi_protocol(connection.read, connection.write))
    }
}
