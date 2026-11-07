// Project Kyber: router.rs
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

use crate::task::Task;

use std::collections::HashMap;
use std::collections::hash_map::Entry;

use bytes::{BufMut, Bytes, BytesMut};
use kymux_util::*;
use kynet::error::*;
use kynet::{Connection, ConnectionStats, RecvStream, SendStream};
#[allow(unused)]
use log::{debug, error, info, warn};
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
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
    datagram_drops: u64,
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
                if let Err(err) = Self::accept_channels_uni(conn2.clone(), clients2).await {
                    error!("{err:?}");
                    conn2.close(1, "accept_uni stopped");
                }
            },
            "accept_uni",
        );

        let conn2 = conn.clone();
        let clients2 = clients.clone();
        let accept_bi_task = Task::spawn_task(
            async move {
                if let Err(err) = Self::accept_channels_bi(conn2.clone(), clients2).await {
                    error!("{err:?}");
                    conn2.close(1, "accept_bi stopped");
                }
            },
            "accept_bi",
        );

        let conn2 = conn.clone();
        let clients2 = clients.clone();
        let recv_datagrams_task = Task::spawn_task(
            async move {
                if let Err(err) = Self::recv_channels_datagrams(conn2.clone(), clients2).await {
                    error!("{err:?}");
                    conn2.close(1, "recv_datagrams stopped");
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
        // Admit a bounded QUIC receive burst without blocking other lanes.
        let (tx_datagrams, rx_datagrams) = mpsc::channel(256);

        let mut clients = self.clients.lock();
        match clients.entry(endpoint_id) {
            Entry::Occupied(_) => return Err(EndpointAlreadyRegistered { endpoint_id }),
            Entry::Vacant(entry) => entry.insert(RouterClient {
                tx_unistreams,
                tx_bistreams,
                tx_datagrams,
                datagram_drops: 0,
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

    async fn read_endpoint_id(recv: &mut RecvStream) -> Result<u16, std::io::Error> {
        let mut endpoint_id_raw = [0u8; 2];
        recv.read_exact(&mut endpoint_id_raw).await?;
        let endpoint = u16::from_be_bytes(endpoint_id_raw);
        Ok(endpoint)
    }

    async fn write_endpoint_id(
        send: &mut SendStream,
        endpoint_id: u16,
    ) -> Result<(), std::io::Error> {
        send.write_all(&endpoint_id.to_be_bytes()).await?;
        send.flush().await
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

    // A slow decoder must not stall other media lanes. These are unreliable
    // datagrams: dropping on bounded admission preserves FEC/recovery semantics.
    // Sequence-gap telemetry accounts for the loss; log local queue pressure
    // separately so it is not mistaken for a network-only failure.
    fn dispatch_datagram(clients: &KyArc<KyMutex<ClientMap>>, datagram: Bytes) {
        if datagram.len() < 2 {
            return;
        }
        let id = u16::from_be_bytes([datagram[0], datagram[1]]);
        let mut clients = clients.lock();
        let Some(client) = clients.get_mut(&id) else {
            return;
        };
        match client.tx_datagrams.try_send(datagram) {
            Ok(()) | Err(mpsc::error::TrySendError::Closed(_)) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                client.datagram_drops = client.datagram_drops.saturating_add(1);
                if client.datagram_drops.is_power_of_two() {
                    warn!(
                        "endpoint {id:X} datagram queue drops={}",
                        client.datagram_drops
                    );
                }
            }
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
            Self::dispatch_datagram(&clients, datagram);
        }
    }
}

impl Drop for Router {
    fn drop(&mut self) {
        let tasks = std::mem::take(&mut self.tasks);
        for task in tasks {
            task.cancel();
        }
    }
}

#[cfg(test)]
mod dispatch_tests {
    use super::*;

    #[tokio::test]
    async fn saturated_or_retired_video_lane_does_not_block_audio() {
        let clients = KyArc::new(KyMutex::new(HashMap::new()));
        let (video_tx, mut video_rx) = mpsc::channel(1);
        let (audio_tx, mut audio_rx) = mpsc::channel(1);
        for (id, tx) in [(2, video_tx), (4, audio_tx)] {
            clients.lock().insert(
                id,
                RouterClient {
                    tx_unistreams: mpsc::channel(1).0,
                    tx_bistreams: mpsc::channel(1).0,
                    tx_datagrams: tx,
                    datagram_drops: 0,
                },
            );
        }
        Router::dispatch_datagram(&clients, Bytes::from_static(&[0, 2, 1]));
        Router::dispatch_datagram(&clients, Bytes::from_static(&[0, 2, 2]));
        Router::dispatch_datagram(&clients, Bytes::from_static(&[0, 4, 3]));
        assert_eq!(audio_rx.try_recv().unwrap()[2], 3);
        assert_eq!(video_rx.try_recv().unwrap()[2], 1);
        assert_eq!(clients.lock().get(&2).unwrap().datagram_drops, 1);
        drop(video_rx);
        Router::dispatch_datagram(&clients, Bytes::from_static(&[0, 2, 4]));
        Router::dispatch_datagram(&clients, Bytes::from_static(&[0, 4, 5]));
        assert_eq!(audio_rx.try_recv().unwrap()[2], 5);
        Router::dispatch_datagram(&clients, Bytes::from_static(&[0]));
        Router::dispatch_datagram(&clients, Bytes::from_static(&[0, 99]));
    }
}

pub(crate) struct KyChannel {
    endpoint_id: u16,
    conn: Connection,
    rx_unistreams: mpsc::Receiver<RecvStream>,
    // Unused for now; kept for symmetry with accept_bi.
    rx_bistreams: mpsc::Receiver<(SendStream, RecvStream)>,
    rx_datagrams: mpsc::Receiver<Bytes>,
    dropper: KyChannelDropper,
}

pub(crate) struct KyChannelSend {
    endpoint_id: u16,
    conn: Connection,
    /// See [`KyChannelDropper`].
    #[expect(dead_code)]
    dropper: KyArc<KyChannelDropper>,
}

pub(crate) struct KyChannelRecv {
    endpoint_id: u16,
    conn: Connection,
    rx_unistreams: mpsc::Receiver<RecvStream>,
    rx_bistreams: mpsc::Receiver<(SendStream, RecvStream)>,
    rx_datagrams: mpsc::Receiver<Bytes>,
    /// See [`KyChannelDropper`].
    #[expect(dead_code)]
    dropper: KyArc<KyChannelDropper>,
}

/// Removes the endpoint from the router on drop.
struct KyChannelDropper {
    endpoint_id: u16,
    clients: KyArc<KyMutex<ClientMap>>, // to implement Drop
}

impl KyChannelSend {
    // Unused for now; kept for API symmetry with KyChannel.
    #[expect(dead_code)]
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

    // See KyChannel::open_bi's comment.
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

    // See KyChannel::open_bi's comment.
    #[expect(dead_code)]
    pub async fn open_bi(&self) -> Result<(SendStream, RecvStream), ConnectionError> {
        Self::open_bi_(&self.conn, self.endpoint_id).await
    }

    #[expect(dead_code)]
    pub async fn send_datagram(&self, data: Bytes) -> Result<(), SendDatagramError> {
        Self::send_datagram_(&self.conn, data).await
    }

    #[expect(dead_code)]
    pub fn max_datagram_size(&self) -> Option<usize> {
        Self::max_datagram_size_(&self.conn)
    }

    #[expect(dead_code)]
    pub fn write_datagram_header(&self, buf: &mut BytesMut) {
        Self::write_datagram_header_(self.endpoint_id, buf);
    }

    #[expect(dead_code)]
    pub async fn connection_stats(&self) -> ConnectionStats {
        KyChannel::connection_stats_(&self.conn).await
    }
}

impl KyChannelRecv {
    // See KyChannelSend::endpoint_id's comment.
    #[expect(dead_code)]
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

    // See KyChannel::accept_bi's comment.
    #[expect(dead_code)]
    pub async fn accept_bi(&mut self) -> Result<(SendStream, RecvStream), RouterError> {
        Self::recv_(&mut self.rx_bistreams).await
    }

    #[expect(dead_code)]
    pub async fn recv_datagram(&mut self) -> Result<Bytes, RouterError> {
        Self::recv_(&mut self.rx_datagrams).await
    }

    #[expect(dead_code)]
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

    // Unused for now; kept for future protocols.
    #[expect(dead_code)]
    pub async fn open_bi(&self) -> Result<(SendStream, RecvStream), ConnectionError> {
        KyChannelSend::open_bi_(&self.conn, self.endpoint_id).await
    }

    pub async fn accept_uni(&mut self) -> Result<RecvStream, RouterError> {
        KyChannelRecv::recv_(&mut self.rx_unistreams).await
    }

    #[expect(dead_code)]
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

    #[expect(dead_code)]
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
