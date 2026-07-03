// Project Kyber: actix_ws.rs
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

use super::{MessageRecvStreamDriver, MessageSendStreamDriver};
use crate::{Connection, MessageRecvStream, MessageSendStream};

use std::io::{Error, ErrorKind, Result};

use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::mpsc;

pub fn new(session: actix_ws::Session, stream: actix_ws::MessageStream) -> Connection {
    let send = MessageSendStream {
        driver: Box::new(ActixWsMessageSendStreamDriver::new(session)),
    };
    let recv = MessageRecvStream {
        driver: Box::new(ActixWsMessageRecvStreamDriver::new(stream)),
    };
    Connection { send, recv }
}

struct ActixWsMessageSendStreamDriver {
    session: actix_ws::Session,
}

// actix_ws::Session does not implement Debug
impl std::fmt::Debug for ActixWsMessageSendStreamDriver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ActixWsMessageSendStreamDriver")
    }
}

impl ActixWsMessageSendStreamDriver {
    fn new(session: actix_ws::Session) -> Self {
        Self { session }
    }
}

#[async_trait]
impl MessageSendStreamDriver for ActixWsMessageSendStreamDriver {
    async fn send(&mut self, msg: Bytes) -> Result<()> {
        self.session
            .binary(msg)
            .await
            .map_err(|err| Error::new(ErrorKind::ConnectionAborted, err))?;
        Ok(())
    }

    async fn close(self: Box<Self>) {
        let _ = self.session.close(None).await;
    }
}

struct ActixWsMessageRecvStreamDriver {
    rx: mpsc::Receiver<Result<Bytes>>,
}

// actix_ws::MessageStream does not implement Debug
impl std::fmt::Debug for ActixWsMessageRecvStreamDriver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ActixWsMessageRecvStreamDriver")
    }
}

impl ActixWsMessageRecvStreamDriver {
    fn new(mut stream: actix_ws::MessageStream) -> Self {
        let (tx, rx) = mpsc::channel(32);
        tokio::task::spawn_local(async move {
            while let Some(res) = stream.recv().await {
                let res = match res {
                    Ok(actix_ws::Message::Binary(bytes)) => Ok(bytes),
                    Ok(actix_ws::Message::Close(None)) => {
                        log::info!("Websocket closed (no reason given)");
                        break;
                    }
                    Ok(actix_ws::Message::Close(Some(close_reason))) => {
                        log::info!(
                            "Websocket closed: {:?} {:?}",
                            close_reason.code,
                            close_reason.description
                        );
                        break;
                    }
                    Ok(msg) => {
                        log::warn!("Unexpected ws message: {msg:?}");
                        break;
                    }
                    Err(err) => Err(actix_to_io_error(err)),
                };
                if tx.send(res).await.is_err() {
                    // Receiver is dropped
                    return;
                }
            }
        });

        Self { rx }
    }
}

#[async_trait]
impl MessageRecvStreamDriver for ActixWsMessageRecvStreamDriver {
    async fn recv(&mut self) -> Result<Option<Bytes>> {
        self.rx.recv().await.transpose()
    }
}

fn actix_to_io_error(err: actix_ws::ProtocolError) -> Error {
    match err {
        actix_ws::ProtocolError::Io(err) => err,
        err => Error::other(err),
    }
}
