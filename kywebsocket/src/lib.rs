// Project Kyber: lib.rs
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

mod driver;
mod endpoint;
mod error;
mod ipc_msg;

use crate::driver::{MessageRecvStreamDriver, MessageSendStreamDriver};
pub use crate::error::*;
pub use endpoint::Channel;

pub use bytes::Bytes;
pub use kymux_types::{ProtocolEndpoint, ProtocolError};
pub use kymux_util::*;

use std::io::Result;

#[derive(Debug)]
pub struct Connection {
    pub send: MessageSendStream,
    pub recv: MessageRecvStream,
}

impl Connection {
    #[cfg(feature = "kywebsocket-js")]
    pub async fn websocket_js_connect(url: &str) -> Result<Self> {
        driver::websocket_js::connect(url).await
    }

    #[cfg(feature = "kywebsocket-actix-ws")]
    pub fn actix_ws_new(session: actix_ws::Session, stream: actix_ws::MessageStream) -> Self {
        driver::actix_ws::new(session, stream)
    }

    pub async fn close(self) {
        self.send.close().await;
    }

    pub fn into_channel(self) -> Channel {
        Channel::from_connection(self)
    }
}

#[derive(Debug)]
pub struct MessageSendStream {
    driver: Box<dyn MessageSendStreamDriver>,
}

impl MessageSendStream {
    pub fn new(driver: impl MessageSendStreamDriver + 'static) -> Self {
        Self {
            driver: Box::new(driver),
        }
    }

    pub async fn send(&mut self, msg: Bytes) -> Result<()> {
        self.driver.send(msg).await
    }

    pub async fn close(self) {
        self.driver.close().await;
    }
}

#[derive(Debug)]
pub struct MessageRecvStream {
    driver: Box<dyn MessageRecvStreamDriver>,
}

impl MessageRecvStream {
    pub fn new(driver: impl MessageRecvStreamDriver + 'static) -> Self {
        Self {
            driver: Box::new(driver),
        }
    }

    pub async fn recv(&mut self) -> Result<Option<Bytes>> {
        self.driver.recv().await
    }
}
