// Project Kyber: router.rs
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

#![allow(unused)] // TODO remove

use crate::runtime;
use crate::task::Task;

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::future::Future;

use bytes::{BufMut, Bytes, BytesMut};
use kymux_util::*;
use kynet::error::*;
use kynet::{Connection, ConnectionStats, RecvStream, SendStream};
#[allow(unused)]
use log::{debug, error, info, warn};
use thiserror::Error;
use tokio::sync::mpsc;

type ClientMap = HashMap<u16, RouterClient>;

#[derive(Debug, Error)]
#[error("router error: {0}")]
pub(crate) struct RouterError(String);

#[derive(Debug, Error)]
#[error("endpoint already registered: {endpoint_id:X}")]
pub(crate) struct EndpointAlreadyRegistered {
    pub endpoint_id: u16,
}

pub(crate) struct RouterClient {
    tx_unistreams: mpsc::Sender<RecvStream>,
    tx_bistreams: mpsc::Sender<(SendStream, RecvStream)>,
    tx_datagrams: mpsc::Sender<Bytes>,
}

pub(crate) struct Router {
    conn: Connection,
    clients: KyArc<KyMutex<ClientMap>>,
    tasks: Vec<Task>,
}

impl Router {
    pub fn start(conn: Connection) -> Self {
        let clients = KyArc::new(KyMutex::new(HashMap::new()));

        let conn2 = conn.clone();
        let clients2 = clients.clone();
        let accept_uni_task = Task::spawn_task(
            async move {
                if let Err(err) = Self::accept_channels_uni(conn2, clients2).await {
                    error!("{err:?}");
                }
            },
            "accept_uni",
        );

        let conn2 = conn.clone();
        let clients2 = clients.clone();
        let accept_bi_task = Task::spawn_task(
            async move {
                if let Err(err) = Self::accept_channels_bi(conn2, clients2).await {
                    error!("{err:?}");
                }
            },
            "accept_bi",
        );

        let conn2 = conn.clone();
        let clients2 = clients.clone();
        let recv_datagrams_task = Task::spawn_task(
            async move {
                if let Err(err) = Self::recv_channels_datagrams(conn2, clients2).await {
                    error!("{err:?}");
                }
            },
            "recv_datagrams",
        );

        let tasks = vec![accept_uni_task, accept_bi_task, recv_datagrams_task];
        Self {
            conn,
            clients,
            tasks,
        }
    }

    pub fn register(&self, endpoint_id: u16) -> Result<KyChannel, EndpointAlreadyRegistered> {
        let (tx_unistreams, rx_unistreams) = mpsc::channel(8);
        let (tx_bistreams, rx_bistreams) = mpsc::channel(8);
        let (tx_datagrams, rx_datagrams) = mpsc::channel(16);

        let mut clients = self.clients.lock();
        match clients.entry(endpoint_id) {
            Entry::Occupied(_) => return Err(EndpointAlreadyRegistered { endpoint_id }),
            Entry::Vacant(entry) => entry.insert(RouterClient {
                tx_unistreams,
                tx_bistreams,
                tx_datagrams,
            }),
        };

        Ok(KyChannel::new(
            endpoint_id,
            self.conn.clone(),
            self.clients.clone(),
            rx_unistreams,
            rx_bistreams,
            rx_datagrams,
        ))
    }

    async fn read_endpoint_id(recv: &mut RecvStream) -> Result<u16, ReadExactError> {
        let mut endpoint_id_raw = [0u8; 2];
        recv.read_exact(&mut endpoint_id_raw).await?;
        let endpoint = u16::from_be_bytes(endpoint_id_raw);
        Ok(endpoint)
    }

    async fn write_endpoint_id(send: &mut SendStream, endpoint_id: u16) -> Result<(), WriteError> {
        send.write_all(&endpoint_id.to_be_bytes()).await
    }

    async fn accept_channels_uni(
        conn: Connection,
        clients: KyArc<KyMutex<ClientMap>>,
    ) -> Result<(), RouterError> {
        loop {
            let mut recv = conn
                .accept_uni()
                .await
                .map_err(|err| RouterError(format!("accept_uni() failed: {err:?}")))?;
            match Self::read_endpoint_id(&mut recv).await {
                Ok(endpoint_id) => {
                    let tx = if let Some(client) = clients.lock().get_mut(&endpoint_id) {
                        Some(client.tx_unistreams.clone())
                    } else {
                        None
                    };

                    if let Some(tx) = tx {
                        tx.send(recv).await.map_err(|err| {
                            RouterError(format!("accept_uni: channel closed: {err:?}"))
                        })?;
                    } else {
                        Err(RouterError(format!(
                            "accept_uni: unknown channel id: {endpoint_id}"
                        )))?;
                    }
                }
                Err(err) => {
                    // This is expected if the stream is already reset
                    debug!("accept_uni: read endpoint error: {err:?}");
                }
            };
        }
    }

    async fn accept_channels_bi(
        conn: Connection,
        clients: KyArc<KyMutex<ClientMap>>,
    ) -> Result<(), RouterError> {
        loop {
            let (send, mut recv) = conn
                .accept_bi()
                .await
                .map_err(|err| RouterError(format!("accept_bi() failed: {err:?}")))?;
            match Self::read_endpoint_id(&mut recv).await {
                Ok(endpoint_id) => {
                    let tx = if let Some(client) = clients.lock().get_mut(&endpoint_id) {
                        Some(client.tx_bistreams.clone())
                    } else {
                        None
                    };

                    if let Some(tx) = tx {
                        tx.send((send, recv)).await.map_err(|err| {
                            RouterError(format!("accept_bi: channel closed: {err:?}"))
                        })?;
                    } else {
                        Err(RouterError(format!(
                            "accept_bi: unknown channel id: {endpoint_id}"
                        )))?;
                    }
                }
                Err(err) => {
                    // This is expected if the stream is already reset
                    debug!("accept_bi: read endpoint error: {err:?}");
                }
            };
        }
    }

    async fn recv_channels_datagrams(
        conn: Connection,
        clients: KyArc<KyMutex<ClientMap>>,
    ) -> Result<(), RouterError> {
        loop {
            let datagram = conn
                .read_datagram()
                .await
                .map_err(|err| RouterError(format!("read_datagram() failed: {err:?}")))?;
            if datagram.len() >= 2 {
                let endpoint_id = u16::from_be_bytes((&datagram[..2]).try_into().unwrap());
                let tx = if let Some(client) = clients.lock().get_mut(&endpoint_id) {
                    Some(client.tx_datagrams.clone())
                } else {
                    None
                };

                if let Some(tx) = tx {
                    tx.send(datagram).await.map_err(|err| {
                        RouterError(format!("recv_datagram: channel closed: {err:?}"))
                    })?;
                } else {
                    // Not a hard error, datagrams may be delivered at any
                    // time, including once the endpoint has been removed
                    warn!("Received a datagram with an unknown id: {endpoint_id:X}");
                }
            } else {
                warn!("Datagram endpoint id too short, dropping");
            }
        }
    }
}

impl Drop for Router {
    fn drop(&mut self) {
        let tasks = std::mem::take(&mut self.tasks);
        for task in tasks {
            let name = task.name.clone();
            let ret = task.cancel();
            if ret.is_err() {
                warn!("Task {name} seems to be already stopped");
            }
        }
    }
}

pub(crate) struct KyChannel {
    endpoint_id: u16,
    conn: Connection,
    rx_unistreams: mpsc::Receiver<RecvStream>,
    rx_bistreams: mpsc::Receiver<(SendStream, RecvStream)>,
    rx_datagrams: mpsc::Receiver<Bytes>,
    dropper: KyChannelDropper,
}

pub(crate) struct KyChannelSend {
    endpoint_id: u16,
    conn: Connection,
    dropper: KyArc<KyChannelDropper>,
}

pub(crate) struct KyChannelRecv {
    endpoint_id: u16,
    conn: Connection,
    rx_unistreams: mpsc::Receiver<RecvStream>,
    rx_bistreams: mpsc::Receiver<(SendStream, RecvStream)>,
    rx_datagrams: mpsc::Receiver<Bytes>,
    dropper: KyArc<KyChannelDropper>,
}

struct KyChannelDropper {
    endpoint_id: u16,
    clients: KyArc<KyMutex<ClientMap>>, // to implement Drop
}

impl KyChannelSend {
    pub fn endpoint_id(&self) -> u16 {
        self.endpoint_id
    }

    async fn open_uni_(conn: &Connection, endpoint_id: u16) -> Result<SendStream, ConnectionError> {
        let mut send = conn.open_uni().await?;
        Router::write_endpoint_id(&mut send, endpoint_id)
            .await
            .map_err(|err| {
                ConnectionError(format!(
                    "Could not write endpoint_id {endpoint_id}: {err:?}"
                ))
            })?;
        Ok(send)
    }

    async fn open_bi_(
        conn: &Connection,
        endpoint_id: u16,
    ) -> Result<(SendStream, RecvStream), ConnectionError> {
        let (mut send, recv) = conn.open_bi().await?;
        Router::write_endpoint_id(&mut send, endpoint_id)
            .await
            .map_err(|err| {
                ConnectionError(format!(
                    "Could not write endpoint_id {endpoint_id}: {err:?}"
                ))
            })?;
        Ok((send, recv))
    }

    async fn send_datagram_(conn: &Connection, data: Bytes) -> Result<(), SendDatagramError> {
        conn.send_datagram(data).await
    }

    fn max_datagram_size_(conn: &Connection) -> Option<usize> {
        conn.max_datagram_size()
    }

    fn write_datagram_header_(endpoint_id: u16, buf: &mut BytesMut) {
        buf.put_u16(endpoint_id);
    }

    pub async fn open_uni(&self) -> Result<SendStream, ConnectionError> {
        Self::open_uni_(&self.conn, self.endpoint_id).await
    }

    pub async fn open_bi(&self) -> Result<(SendStream, RecvStream), ConnectionError> {
        Self::open_bi_(&self.conn, self.endpoint_id).await
    }

    pub async fn send_datagram(&self, data: Bytes) -> Result<(), SendDatagramError> {
        Self::send_datagram_(&self.conn, data).await
    }

    pub fn max_datagram_size(&self) -> Option<usize> {
        Self::max_datagram_size_(&self.conn)
    }

    pub fn write_datagram_header(&self, buf: &mut BytesMut) {
        Self::write_datagram_header_(self.endpoint_id, buf);
    }

    pub async fn connection_stats(&self) -> ConnectionStats {
        KyChannel::connection_stats_(&self.conn).await
    }
}

impl KyChannelRecv {
    pub fn endpoint_id(&self) -> u16 {
        self.endpoint_id
    }

    async fn recv_<T>(rx: &mut mpsc::Receiver<T>) -> Result<T, RouterError> {
        rx.recv()
            .await
            .ok_or_else(|| RouterError("Ky channel closed".to_string()))
    }

    pub async fn accept_uni(&mut self) -> Result<RecvStream, RouterError> {
        Self::recv_(&mut self.rx_unistreams).await
    }

    pub async fn accept_bi(&mut self) -> Result<(SendStream, RecvStream), RouterError> {
        Self::recv_(&mut self.rx_bistreams).await
    }

    pub async fn recv_datagram(&mut self) -> Result<Bytes, RouterError> {
        Self::recv_(&mut self.rx_datagrams).await
    }

    pub async fn connection_stats(&self) -> ConnectionStats {
        KyChannel::connection_stats_(&self.conn).await
    }
}

impl KyChannel {
    pub(crate) fn new(
        endpoint_id: u16,
        conn: Connection,
        clients: KyArc<KyMutex<ClientMap>>,
        rx_unistreams: mpsc::Receiver<RecvStream>,
        rx_bistreams: mpsc::Receiver<(SendStream, RecvStream)>,
        rx_datagrams: mpsc::Receiver<Bytes>,
    ) -> Self {
        Self {
            endpoint_id,
            conn,
            rx_unistreams,
            rx_bistreams,
            rx_datagrams,
            dropper: KyChannelDropper {
                endpoint_id,
                clients,
            },
        }
    }

    pub fn endpoint_id(&self) -> u16 {
        self.endpoint_id
    }

    pub async fn open_uni(&self) -> Result<SendStream, ConnectionError> {
        KyChannelSend::open_uni_(&self.conn, self.endpoint_id).await
    }

    pub async fn open_bi(&self) -> Result<(SendStream, RecvStream), ConnectionError> {
        KyChannelSend::open_bi_(&self.conn, self.endpoint_id).await
    }

    pub async fn accept_uni(&mut self) -> Result<RecvStream, RouterError> {
        KyChannelRecv::recv_(&mut self.rx_unistreams).await
    }

    pub async fn accept_bi(&mut self) -> Result<(SendStream, RecvStream), RouterError> {
        KyChannelRecv::recv_(&mut self.rx_bistreams).await
    }

    pub async fn send_datagram(&self, data: Bytes) -> Result<(), SendDatagramError> {
        KyChannelSend::send_datagram_(&self.conn, data).await
    }

    pub async fn recv_datagram(&mut self) -> Result<Bytes, RouterError> {
        KyChannelRecv::recv_(&mut self.rx_datagrams).await
    }

    pub fn max_datagram_size(&self) -> Option<usize> {
        KyChannelSend::max_datagram_size_(&self.conn)
    }

    pub fn write_datagram_header(&self, buf: &mut BytesMut) {
        KyChannelSend::write_datagram_header_(self.endpoint_id, buf);
    }

    pub async fn connection_stats_(conn: &Connection) -> ConnectionStats {
        conn.stats().await
    }

    pub async fn connection_stats(&self) -> ConnectionStats {
        Self::connection_stats_(&self.conn).await
    }

    pub fn into_split(self) -> (KyChannelRecv, KyChannelSend) {
        let KyChannel {
            endpoint_id,
            conn,
            rx_unistreams,
            rx_bistreams,
            rx_datagrams,
            dropper,
        } = self;

        let dropper = KyArc::new(dropper);

        let send = KyChannelSend {
            endpoint_id,
            conn: conn.clone(),
            dropper: dropper.clone(),
        };

        let recv = KyChannelRecv {
            endpoint_id,
            conn,
            rx_unistreams,
            rx_bistreams,
            rx_datagrams,
            dropper,
        };

        (recv, send)
    }
}

impl Drop for KyChannelDropper {
    fn drop(&mut self) {
        let mut clients = self.clients.lock();
        let ret = clients.remove(&self.endpoint_id);
        // An id must not be reused, so it's not racy
        assert!(ret.is_some());
    }
}
