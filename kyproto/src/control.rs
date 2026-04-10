// Project Kyber: control.rs
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

use crate::task::Task;

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};

use kymux_util::*;
use kynet::error::*;
use kynet::{RecvStream, SendStream};
#[allow(unused)]
use log::{debug, error, info, warn};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};

#[derive(Debug, Error)]
#[error("control error: {0}")]
pub(crate) struct ControlError(String);

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum ControlMsg {
    RegisterEndpoint { endpoint_id: u16 },
    EndpointRegistered { endpoint_id: u16 },
    Ready { endpoint_id: u16 },
}

pub(crate) struct ReadyNotifier {
    endpoint_id: u16,
    tx: mpsc::Sender<ControlMsg>,
    rx: oneshot::Receiver<()>,
}

impl ReadyNotifier {
    pub async fn ready(self) -> Result<(), ControlError> {
        // Notify the other peer that we are ready
        let msg = ControlMsg::Ready {
            endpoint_id: self.endpoint_id,
        };
        self.tx.send(msg).await?;
        // Await the other peer to be ready
        self.rx.await?;

        Ok(())
    }
}

enum PendingReady {
    AwaitingLocal,
    AwaitingRemote(oneshot::Sender<()>),
}

struct State {
    pending_register_ack: KyMutex<HashMap<u16, oneshot::Sender<()>>>,
    pending_connect: KyMutex<HashSet<u16>>,
    pending_ready: KyMutex<HashMap<u16, PendingReady>>,
}

impl State {
    fn new() -> Self {
        Self {
            pending_register_ack: KyMutex::new(HashMap::new()),
            pending_connect: KyMutex::new(HashSet::new()),
            pending_ready: KyMutex::new(HashMap::new()),
        }
    }
}

pub(crate) struct Control {
    tx: mpsc::Sender<ControlMsg>,
    state: KyArc<State>,
    tasks: Vec<Task>,
}

impl Control {
    pub(crate) fn start(stream_tx: SendStream, stream_rx: RecvStream) -> Self {
        let state = KyArc::new(State::new());
        let (control_tx, control_rx) = mpsc::channel(8);

        let control_tx2 = control_tx.clone();
        let state2 = state.clone();
        let rx_task = Task::spawn_task(
            async move {
                if let Err(err) = Self::recv_msgs(stream_rx, state2, control_tx2).await {
                    error!("{err:?}");
                }
            },
            "control recv_msgs",
        );

        let tx_task = Task::spawn_task(
            async move {
                if let Err(err) = Self::send_msgs(control_rx, stream_tx).await {
                    error!("{err:?}");
                }
            },
            "control send_msgs",
        );

        let tasks = vec![rx_task, tx_task];
        Self {
            tx: control_tx,
            state,
            tasks,
        }
    }

    pub(crate) async fn register_endpoint(&self, endpoint_id: u16) -> Result<(), ControlError> {
        let register_ack_recv = {
            let mut pending_register_ack = self.state.pending_register_ack.lock();
            match pending_register_ack.entry(endpoint_id) {
                Entry::Occupied(_) => Err(ControlError(format!(
                    "Endpoint already pending: {endpoint_id:X}"
                )))?,
                Entry::Vacant(entry) => {
                    let (tx, rx) = oneshot::channel();
                    entry.insert(tx);
                    rx
                }
            }
        };

        let msg = ControlMsg::RegisterEndpoint { endpoint_id };
        self.tx.send(msg).await?;

        register_ack_recv.await?;
        Ok(())
    }

    pub(crate) fn register_ready_notifier(
        &self,
        endpoint_id: u16,
    ) -> Result<ReadyNotifier, ControlError> {
        let mut pending_ready = self.state.pending_ready.lock();
        match pending_ready.entry(endpoint_id) {
            Entry::Occupied(entry) => {
                match entry.get() {
                    PendingReady::AwaitingRemote(_) => Err(ControlError(format!(
                        "Endpoint already registered for ready notifications: {endpoint_id:X}"
                    )))?,
                    PendingReady::AwaitingLocal => {
                        entry.remove_entry();

                        // The Ready message has already been received from the peer
                        let (tx, rx) = oneshot::channel();
                        tx.send(())
                            .map_err(|_| ControlError("Ready sender error".to_string()))?;

                        Ok(ReadyNotifier {
                            endpoint_id,
                            tx: self.tx.clone(),
                            rx,
                        })
                    }
                }
            }
            Entry::Vacant(entry) => {
                let (tx, rx) = oneshot::channel();
                entry.insert(PendingReady::AwaitingRemote(tx));
                Ok(ReadyNotifier {
                    endpoint_id,
                    tx: self.tx.clone(),
                    rx,
                })
            }
        }
    }

    async fn send_msgs(
        mut control_rx: mpsc::Receiver<ControlMsg>,
        mut stream_tx: SendStream,
    ) -> Result<(), ControlError> {
        while let Some(msg) = control_rx.recv().await {
            Self::send_msg(&mut stream_tx, &msg).await?;
        }

        Ok(())
    }

    async fn send_msg(tx: &mut SendStream, msg: &ControlMsg) -> Result<(), ControlError> {
        let buf = rmp_serde::to_vec(&msg)?;
        let len = u32::try_from(buf.len()).unwrap();

        tx.write_all(&len.to_be_bytes()).await?;
        tx.write_all(&buf).await?;
        Ok(())
    }

    async fn recv_msgs(
        mut rx: RecvStream,
        state: KyArc<State>,
        control_tx: mpsc::Sender<ControlMsg>,
    ) -> Result<(), ControlError> {
        while let Some(msg) = Self::recv_msg(&mut rx).await? {
            if let Err(err) = Self::handle_msg(msg, &state, &control_tx).await {
                // not fatal
                error!("Could not handle control message: {err}");
            }
        }

        Ok(())
    }

    async fn recv_msg(rx: &mut RecvStream) -> Result<Option<ControlMsg>, ControlError> {
        let mut buf = [0u8; 4];
        let res = rx.read_exact(&mut buf).await;
        if let Err(ReadExactError::FinishedEarly(read)) = res {
            if read == 0 {
                return Ok(None);
            }
        }
        res?;

        let len = u32::from_be_bytes(buf);
        let mut buf = vec![0u8; len as usize];
        rx.read_exact(&mut buf).await?;

        let msg = rmp_serde::from_slice(&buf)?;

        Ok(Some(msg))
    }

    async fn handle_msg(
        msg: ControlMsg,
        state: &KyArc<State>,
        control_tx: &mpsc::Sender<ControlMsg>,
    ) -> Result<(), ControlError> {
        match msg {
            ControlMsg::RegisterEndpoint { endpoint_id } => {
                {
                    let mut pending_connect = state.pending_connect.lock();
                    if !pending_connect.insert(endpoint_id) {
                        Err(ControlError(format!(
                            "Endpoint already registered: {endpoint_id:X}"
                        )))?;
                    }
                }
                let msg = ControlMsg::EndpointRegistered { endpoint_id };
                control_tx.send(msg).await?;
            }
            ControlMsg::EndpointRegistered { endpoint_id } => {
                let mut pending_register_ack = state.pending_register_ack.lock();
                if let Some(tx) = pending_register_ack.remove(&endpoint_id) {
                    tx.send(()).map_err(|_| {
                        ControlError("Endpoint registered sender error".to_string())
                    })?;
                } else {
                    Err(ControlError(format!(
                        "Received unexpected registration ack for endpoint {endpoint_id:X}"
                    )))?;
                }
            }
            ControlMsg::Ready { endpoint_id } => {
                let mut pending_ready = state.pending_ready.lock();
                match pending_ready.remove(&endpoint_id) {
                    Some(PendingReady::AwaitingRemote(tx)) => {
                        // Notify the client that the remote is ready
                        tx.send(())
                            .map_err(|_| ControlError("Ready sender error".to_string()))?;
                    }
                    Some(PendingReady::AwaitingLocal) => Err(ControlError(format!(
                        "Received multiple ready notifications for endpoint {endpoint_id:X}"
                    )))?,
                    None => {
                        // Ready message received from the peer before the
                        // client connected to the endpoint
                        let old = pending_ready.insert(endpoint_id, PendingReady::AwaitingLocal);
                        assert!(old.is_none());
                    }
                }
            }
        }

        Ok(())
    }
}

impl Drop for Control {
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

// ControlError is internal, there is no need to expose a structured error, so
// convert any error to a String
macro_rules! impl_control_error_from {
    ($t:ty) => {
        impl From<$t> for ControlError {
            fn from(err: $t) -> Self {
                Self(err.to_string())
            }
        }
    };
}
impl_control_error_from!(ReadExactError);
impl_control_error_from!(WriteError);
impl_control_error_from!(rmp_serde::decode::Error);
impl_control_error_from!(rmp_serde::encode::Error);

impl<T> From<mpsc::error::SendError<T>> for ControlError {
    fn from(err: mpsc::error::SendError<T>) -> Self {
        Self(format!("mpsc::Sender error: {err}"))
    }
}

impl From<oneshot::error::RecvError> for ControlError {
    fn from(err: oneshot::error::RecvError) -> Self {
        Self(format!("oneshot::Receiver error: {err}"))
    }
}
