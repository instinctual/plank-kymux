// Project Kyber: server.rs
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

use crate::connection::{Connection, Server};
use crate::endpoint::Channel;
use crate::ipc;
use crate::serial;
use crate::KyComAddr;

use async_trait::async_trait;
use kymux_types as types;
use kymux_types::ProtocolError;
#[allow(unused)]
use log::{debug, error, info, warn};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::io::{Error, ErrorKind};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::io::AsyncWriteExt;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

type EndpointMap = HashMap<u16, oneshot::Sender<std::io::Result<Connection>>>;

pub struct KyCom {
    addr: SocketAddr,
    // Some() initially, then None once accept() fails
    pending_endpoints: Arc<Mutex<Option<EndpointMap>>>,
    listen_task: JoinHandle<()>,
    runner: Option<Runner>,
}

impl KyCom {
    fn new(server: Server) -> Self {
        let addr = server.addr;

        let pending_endpoints = Arc::new(Mutex::new(Some(HashMap::new())));
        let pending_endpoints2 = pending_endpoints.clone();
        let listen_task = tokio::spawn(async move {
            if let Err(err) = Self::listen(server, pending_endpoints2).await {
                error!("Kycom server listener error: {err}");
            }
        });

        Self {
            addr,
            pending_endpoints,
            listen_task,
            runner: None,
        }
    }

    pub async fn start_on_addr(addr: SocketAddr) -> std::io::Result<Self> {
        let server = Server::start_on_addr(addr).await?;
        Ok(Self::new(server))
    }

    pub async fn start_on_port(port: u16) -> std::io::Result<Self> {
        let server = Server::start_on_port(port).await?;
        Ok(Self::new(server))
    }

    pub async fn start_on_any_port(local_ports: std::ops::Range<u16>) -> std::io::Result<Self> {
        let server = Server::start_on_any_port(local_ports).await?;
        Ok(Self::new(server))
    }

    pub fn register_channel(&self, endpoint_id: u16) -> std::io::Result<Channel> {
        let rx = {
            let mut pending_endpoints = self.pending_endpoints.lock().unwrap();
            let Some(pending_endpoints) = pending_endpoints.as_mut() else {
                return Err(ErrorKind::ConnectionAborted.into());
            };
            match pending_endpoints.entry(endpoint_id) {
                Entry::Occupied(_) => {
                    return Err(Error::new(
                        ErrorKind::AlreadyExists,
                        format!("Endpoint {endpoint_id} already pending"),
                    ));
                }
                Entry::Vacant(entry) => {
                    let (tx, rx) = oneshot::channel();
                    entry.insert(tx);
                    rx
                }
            }
        };

        let addr = KyComAddr::new(self.addr, endpoint_id);
        let channel = Channel::new(addr, rx);
        Ok(channel)
    }

    pub fn register<T>(
        &self,
        endpoint: types::ProtocolEndpoint<T>,
    ) -> std::io::Result<TcpForwarder<T>> {
        let rx = {
            let endpoint_id = endpoint.id();
            let mut pending_endpoints = self.pending_endpoints.lock().unwrap();
            let Some(pending_endpoints) = pending_endpoints.as_mut() else {
                return Err(ErrorKind::ConnectionAborted.into());
            };
            match pending_endpoints.entry(endpoint_id) {
                Entry::Occupied(_) => {
                    return Err(Error::new(
                        ErrorKind::AlreadyExists,
                        format!("Endpoint {endpoint_id} already pending"),
                    ));
                }
                Entry::Vacant(entry) => {
                    let (tx, rx) = oneshot::channel();
                    entry.insert(tx);
                    rx
                }
            }
        };

        Ok(TcpForwarder {
            addr: self.addr,
            rx,
            endpoint,
        })
    }

    pub fn forward_async<P>(&mut self, forwarder: TcpForwarder<P>) -> KyComAddr
    where
        P: 'static,
        TcpForwarder<P>: Forwarder,
    {
        let addr = forwarder.addr();

        if self.runner.is_none() {
            // Create on first use
            self.runner = Some(Runner::new());
        }

        let runner = self.runner.as_mut().unwrap();
        runner.forward_async(forwarder);
        addr
    }

    pub fn register_and_forward<T: 'static>(
        &mut self,
        endpoint: types::ProtocolEndpoint<T>,
    ) -> std::io::Result<KyComAddr>
    where
        TcpForwarder<T>: Forwarder,
    {
        let forwarder = self.register(endpoint)?;
        let addr = self.forward_async(forwarder);
        Ok(addr)
    }

    async fn listen(
        mut server: Server,
        pending_endpoints: Arc<Mutex<Option<EndpointMap>>>,
    ) -> std::io::Result<()> {
        loop {
            let connection = match server.accept().await {
                Ok(connection) => connection,
                Err(err) => {
                    let mut pending_endpoints = pending_endpoints.lock().unwrap();
                    // take() sets pending_endpoints to None
                    let pending_endpoints = pending_endpoints.take().expect("Unexpectedly state");
                    for tx in pending_endpoints.into_values() {
                        // Notify all pending endpoints
                        let _ = tx.send(Err(std::io::ErrorKind::ConnectionAborted.into()));
                    }
                    return Err(err);
                }
            };
            let endpoint_id = connection.addr.endpoint_id;

            let mut pending_endpoints = pending_endpoints.lock().unwrap();
            let pending_endpoints = pending_endpoints.as_mut().expect("Unexpectedly state");
            if let Some(tx) = pending_endpoints.remove(&endpoint_id) {
                // Ignore error (if the receiver is dropped)
                let _ = tx.send(Ok(connection));
            } else {
                return Err(Error::new(
                    ErrorKind::NotFound,
                    format!("Connection received for unknown endpoint id: {endpoint_id:X}"),
                ));
            }
        }
    }

    pub fn stop(mut self) {
        // drop self
        if let Some(runner) = self.runner.take() {
            runner.stop();
        }
    }
}

impl Drop for KyCom {
    fn drop(&mut self) {
        self.listen_task.abort();
    }
}

struct Task {
    join_handle: JoinHandle<()>,
}

impl Drop for Task {
    fn drop(&mut self) {
        self.join_handle.abort();
    }
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
        self.tasks.retain(|task| !task.join_handle.is_finished());

        let forwarder_name = std::any::type_name::<T>()
            .rsplit("::")
            .next()
            .unwrap_or("Unknown");
        debug!("Spawning {forwarder_name} forwarder");

        let join_handle = tokio::spawn(async move {
            if let Err(err) = forwarder.forward().await {
                info!("{forwarder_name} forwarder ended with result {err:?}");
            }
        });

        self.tasks.push(Task { join_handle });
    }

    pub fn stop(self) {
        for task in self.tasks {
            task.join_handle.abort();
        }
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

pub struct TcpForwarder<P> {
    addr: SocketAddr,
    rx: oneshot::Receiver<std::io::Result<Connection>>,
    endpoint: types::ProtocolEndpoint<P>,
}

impl<P> TcpForwarder<P> {
    pub fn addr(&self) -> KyComAddr {
        KyComAddr::new(self.addr, self.endpoint.id())
    }

    async fn start(self) -> Result<(Connection, P), ProtocolError> {
        let mut connection = self
            .rx
            .await
            .map_err(|_| ProtocolError::new("Connection sender dropped"))?
            .map_err(ProtocolError::new)?;

        let protocol = self.endpoint.ready().await?;
        connection
            .write_all(&[0])
            .await
            .map_err(ProtocolError::new)?;

        Ok((connection, protocol))
    }
}

#[async_trait]
pub trait Forwarder {
    async fn forward(self) -> Result<(), ProtocolError>;
}

#[async_trait]
impl Forwarder for TcpForwarder<types::VideoClientProtocol> {
    async fn forward(self) -> Result<(), ProtocolError> {
        let (connection, protocol) = self.start().await?;
        let ipc = ipc::create_send_protocol(connection, serial::av::AVPacketSerializer);
        forward_protocol(protocol.recv, ipc).await
    }
}

#[async_trait]
impl Forwarder for TcpForwarder<types::VideoServerProtocol> {
    async fn forward(self) -> Result<(), ProtocolError> {
        let (connection, protocol) = self.start().await?;
        let ipc = ipc::create_recv_protocol(connection, serial::av::AVPacketDeserializer);
        forward_protocol(ipc, protocol.send).await
    }
}

#[async_trait]
impl Forwarder for TcpForwarder<types::AudioClientProtocol> {
    async fn forward(self) -> Result<(), ProtocolError> {
        let (connection, protocol) = self.start().await?;
        let ipc = ipc::create_send_protocol(connection, serial::av::AVPacketSerializer);
        forward_protocol(protocol.recv, ipc).await
    }
}

#[async_trait]
impl Forwarder for TcpForwarder<types::AudioServerProtocol> {
    async fn forward(self) -> Result<(), ProtocolError> {
        let (connection, protocol) = self.start().await?;
        let ipc = ipc::create_recv_protocol(connection, serial::av::AVPacketDeserializer);
        forward_protocol(ipc, protocol.send).await
    }
}

#[async_trait]
impl Forwarder for TcpForwarder<types::InputProtocol> {
    async fn forward(self) -> Result<(), ProtocolError> {
        let (connection, protocol) = self.start().await?;
        let (ipc_send, ipc_recv) = ipc::create_bi_protocol(
            connection.read,
            connection.write,
            serial::input::InputPacketSerializer,
            serial::input::InputPacketDeserializer,
        );
        forward_protocol_bi(protocol.send, protocol.recv, ipc_send, ipc_recv).await
    }
}

#[async_trait]
impl Forwarder for TcpForwarder<types::DataProtocol> {
    async fn forward(self) -> Result<(), ProtocolError> {
        let (connection, protocol) = self.start().await?;
        let (ipc_send, ipc_recv) = ipc::create_bi_protocol(
            connection.read,
            connection.write,
            serial::data::DataPacketSerializer,
            serial::data::DataPacketDeserializer,
        );
        forward_protocol_bi(protocol.send, protocol.recv, ipc_send, ipc_recv).await
    }
}

#[async_trait]
impl Forwarder for TcpForwarder<types::MetricsServerProtocol> {
    async fn forward(self) -> Result<(), ProtocolError> {
        let (connection, protocol) = self.start().await?;
        let ipc = ipc::create_recv_protocol(connection, serial::metrics::MetricsPacketDeserializer);
        forward_protocol(ipc, protocol.send).await
    }
}
