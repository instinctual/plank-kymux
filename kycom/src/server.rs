// Project Kyber: server.rs
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

use crate::connection::{Connection, Server};
use crate::endpoint::{Channel, ChannelRole};
use crate::{KyComAddr, Task};

use async_trait::async_trait;
use kymux_types as types;
use kymux_types::ProtocolError;
#[allow(unused)]
use log::{debug, error, info, warn};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;

#[derive(Default)]
struct EndpointMap {
    next_id: u16,
    map: HashMap<u16, oneshot::Sender<io::Result<Connection>>>,
}

struct Guard<'a>(&'a Mutex<Option<EndpointMap>>);

impl Drop for Guard<'_> {
    fn drop(&mut self) {
        let mut pending_endpoints = self.0.lock().unwrap();
        // take() sets pending_endpoints to None
        let pending_endpoints = pending_endpoints.take().unwrap();
        for tx in pending_endpoints.map.into_values() {
            // Notify all pending endpoints
            let _ = tx.send(Err(io::ErrorKind::ConnectionAborted.into()));
        }
    }
}

pub struct KyCom {
    addr: SocketAddr,
    // Some() initially, then None once accept() fails
    pending_endpoints: Arc<Mutex<Option<EndpointMap>>>,
    _listen_task: Task,
    runner: Runner,
}

impl KyCom {
    fn new(server: Server) -> Self {
        let addr = server.addr;

        let pending_endpoints = Arc::new(Mutex::new(Some(EndpointMap::default())));
        let pending_endpoints2 = pending_endpoints.clone();
        let _listen_task = Task::spawn(async move {
            if let Err(err) = Self::listen(server, pending_endpoints2).await {
                error!("Kycom server listener error: {err}");
            }
        });

        Self {
            addr,
            pending_endpoints,
            _listen_task,
            runner: Runner::new(),
        }
    }

    pub async fn start_on_addr(addr: SocketAddr) -> io::Result<Self> {
        let server = Server::start_on_addr(addr).await?;
        Ok(Self::new(server))
    }

    pub async fn start_on_port(port: u16) -> io::Result<Self> {
        let server = Server::start_on_port(port).await?;
        Ok(Self::new(server))
    }

    pub async fn start_on_any_port(local_ports: std::ops::Range<u16>) -> io::Result<Self> {
        let server = Server::start_on_any_port(local_ports).await?;
        Ok(Self::new(server))
    }

    pub fn register_channel(&self) -> io::Result<Channel> {
        let mut pending_endpoints = self.pending_endpoints.lock().unwrap();
        let Some(pending_endpoints) = pending_endpoints.as_mut() else {
            return Err(io::ErrorKind::ConnectionAborted.into());
        };

        // If all ids are taken, the loop would be infinite
        assert!(pending_endpoints.map.len() != 1 << 16);

        let (tx, rx) = oneshot::channel();
        let endpoint_id = loop {
            let endpoint_id = pending_endpoints.next_id;
            pending_endpoints.next_id = pending_endpoints.next_id.wrapping_add(1);

            // The endpoint id may already be used if `next_id` has wrapped
            match pending_endpoints.map.entry(endpoint_id) {
                Entry::Occupied(_) => continue,
                Entry::Vacant(entry) => {
                    entry.insert(tx);
                    break endpoint_id;
                }
            }
        };

        let addr = KyComAddr::new(self.addr, endpoint_id);
        let channel = Channel::new(addr, rx, ChannelRole::Server);
        Ok(channel)
    }

    pub fn forward<P>(&mut self, channel: Channel, endpoint: types::ProtocolEndpoint<P>)
    where
        P: 'static,
        ChannelForwarder<P>: Forwarder,
    {
        self.runner
            .forward_async(ChannelForwarder { channel, endpoint });
    }

    pub fn register_and_forward<T: 'static>(
        &mut self,
        endpoint: types::ProtocolEndpoint<T>,
    ) -> io::Result<KyComAddr>
    where
        ChannelForwarder<T>: Forwarder,
    {
        let channel = self.register_channel()?;
        let addr = channel.addr();
        self.forward(channel, endpoint);
        Ok(addr)
    }

    async fn listen(
        mut server: Server,
        pending_endpoints: Arc<Mutex<Option<EndpointMap>>>,
    ) -> io::Result<()> {
        let _guard = Guard(&pending_endpoints);

        loop {
            let connection = server.accept().await?;
            let endpoint_id = connection.addr.endpoint_id;

            let mut pending_endpoints = pending_endpoints.lock().unwrap();
            let pending_endpoints = pending_endpoints.as_mut().unwrap();
            if let Some(tx) = pending_endpoints.map.remove(&endpoint_id) {
                // Ignore error (if the receiver is dropped)
                let _ = tx.send(Ok(connection));
            } else {
                warn!("Connection received for unknown endpoint id: {endpoint_id:X}");
            }
        }
    }

    /// Dummy function kept for compatibility.
    pub fn stop(self) {}
}

struct Runner {
    tasks: Vec<Task>,
}

impl Runner {
    fn new() -> Self {
        Self { tasks: Vec::new() }
    }

    fn forward_async<T>(&mut self, forwarder: T)
    where
        T: Forwarder + Send + 'static,
    {
        // Clean up finished tasks
        self.tasks.retain(|task| !task.0.is_finished());

        let forwarder_name = std::any::type_name::<T>()
            .rsplit("::")
            .next()
            .unwrap_or("Unknown");
        debug!("Spawning {forwarder_name} forwarder");

        let join_handle = Task::spawn(async move {
            if let Err(err) = forwarder.forward().await {
                info!("{forwarder_name} forwarder ended with result {err:?}");
            }
        });

        self.tasks.push(join_handle);
    }
}

pub async fn forward_protocol<T>(
    mut input: types::ProtocolRecv<T>,
    mut output: types::ProtocolSend<T>,
) -> Result<(), ProtocolError> {
    while let Some(packet) = input.recv().await? {
        output.send(packet).await?;
    }

    Ok(())
}

pub async fn forward_protocol_bi<TX: Send + 'static, RX: Send + 'static>(
    proto_send: types::ProtocolSend<RX>,
    proto_recv: types::ProtocolRecv<TX>,
    ipc_send: types::ProtocolSend<TX>,
    ipc_recv: types::ProtocolRecv<RX>,
) -> Result<(), ProtocolError> {
    tokio::select! {
        res = forward_protocol(ipc_recv, proto_send) => res,
        res = forward_protocol(proto_recv, ipc_send) => res,
    }
    .map_err(ProtocolError::new)
}

pub struct ChannelForwarder<P> {
    channel: Channel,
    endpoint: types::ProtocolEndpoint<P>,
}

#[async_trait]
pub trait Forwarder {
    async fn forward(self) -> Result<(), ProtocolError>;
}

#[async_trait]
impl Forwarder for ChannelForwarder<types::VideoClientProtocol> {
    async fn forward(self) -> Result<(), ProtocolError> {
        let protocol = self.endpoint.ready().await?;
        let ipc = self.channel.into_video_server_endpoint().ready().await?;
        forward_protocol(protocol.recv, ipc.send).await
    }
}

#[async_trait]
impl Forwarder for ChannelForwarder<types::VideoServerProtocol> {
    async fn forward(self) -> Result<(), ProtocolError> {
        let protocol = self.endpoint.ready().await?;
        let ipc = self.channel.into_video_client_endpoint().ready().await?;
        forward_protocol(ipc.recv, protocol.send).await
    }
}

#[async_trait]
impl Forwarder for ChannelForwarder<types::AudioClientProtocol> {
    async fn forward(self) -> Result<(), ProtocolError> {
        let protocol = self.endpoint.ready().await?;
        let ipc = self.channel.into_audio_server_endpoint().ready().await?;
        forward_protocol(protocol.recv, ipc.send).await
    }
}

#[async_trait]
impl Forwarder for ChannelForwarder<types::AudioServerProtocol> {
    async fn forward(self) -> Result<(), ProtocolError> {
        let protocol = self.endpoint.ready().await?;
        let ipc = self.channel.into_audio_client_endpoint().ready().await?;
        forward_protocol(ipc.recv, protocol.send).await
    }
}

#[async_trait]
impl Forwarder for ChannelForwarder<types::InputProtocol> {
    async fn forward(self) -> Result<(), ProtocolError> {
        let protocol = self.endpoint.ready().await?;
        let ipc = self.channel.into_input_endpoint().ready().await?;
        forward_protocol_bi(protocol.send, protocol.recv, ipc.send, ipc.recv).await
    }
}

#[async_trait]
impl Forwarder for ChannelForwarder<types::DataProtocol> {
    async fn forward(self) -> Result<(), ProtocolError> {
        let protocol = self.endpoint.ready().await?;
        let ipc = self.channel.into_data_endpoint().ready().await?;
        forward_protocol_bi(protocol.send, protocol.recv, ipc.send, ipc.recv).await
    }
}

#[async_trait]
impl Forwarder for ChannelForwarder<types::MetricsServerProtocol> {
    async fn forward(self) -> Result<(), ProtocolError> {
        let protocol = self.endpoint.ready().await?;
        let ipc = self.channel.into_metrics_client_endpoint().ready().await?;
        forward_protocol(ipc.recv, protocol.send).await
    }
}
