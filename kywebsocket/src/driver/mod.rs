// Project Kyber: mod.rs
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

#[cfg(all(feature = "kywebsocket-js", target_family = "wasm"))]
pub(crate) mod websocket_js;
#[cfg(all(feature = "kywebsocket-js", not(target_family = "wasm")))]
compile_error!("WebSocketJS is only available for wasm");

#[cfg(all(feature = "kywebsocket-actix-ws", not(target_family = "wasm")))]
pub(crate) mod actix_ws;
#[cfg(all(feature = "kywebsocket-actix-ws", target_family = "wasm"))]
compile_error!("ActixWS is not available for wasm");

#[cfg(all(feature = "kywebsocket-tungstenite", not(target_family = "wasm")))]
pub(crate) mod tungstenite;
#[cfg(all(feature = "kywebsocket-tungstenite", target_family = "wasm"))]
compile_error!("Tungstenite is not available for wasm");

pub use kymux_util::*;

use std::fmt::Debug;
use std::io::Result;

use async_trait::async_trait;
use bytes::Bytes;

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
pub trait MessageSendStreamDriver: Debug + KySend + KySync {
    async fn send(&mut self, msg: Bytes) -> Result<()>;

    async fn close(self: Box<Self>);
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
pub trait MessageRecvStreamDriver: Debug + KySend + KySync {
    async fn recv(&mut self) -> Result<Option<Bytes>>;
}
