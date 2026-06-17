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

pub mod av;
pub mod data;
pub mod input;
pub mod metrics;
pub mod runtime;

pub use av::*;
pub use data::*;
pub use input::*;
pub use metrics::*;

use async_trait::async_trait;
use kymux_util::*;
use thiserror::Error;

#[derive(Debug, Error, Clone)]
#[error("Protocol error: {0}")]
pub struct ProtocolError(pub String);

impl ProtocolError {
    pub fn new<T: ToString>(value: T) -> Self {
        Self(value.to_string())
    }
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
pub trait ProtocolSendDriver: KySend {
    type Packet;

    async fn send(&mut self, packet: Self::Packet) -> Result<(), ProtocolError>;
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
pub trait ProtocolRecvDriver: KySend {
    type Packet;

    async fn recv(&mut self) -> Result<Option<Self::Packet>, ProtocolError>;
}

pub struct ProtocolSend<T> {
    driver: Box<dyn ProtocolSendDriver<Packet = T>>,
}

impl<T> ProtocolSend<T> {
    pub fn new(driver: impl ProtocolSendDriver<Packet = T> + 'static) -> Self {
        Self {
            driver: Box::new(driver),
        }
    }

    pub async fn send(&mut self, packet: T) -> Result<(), ProtocolError> {
        self.driver.send(packet).await
    }
}

pub struct ProtocolRecv<T> {
    driver: Box<dyn ProtocolRecvDriver<Packet = T>>,
}

impl<T> ProtocolRecv<T> {
    pub fn new(driver: impl ProtocolRecvDriver<Packet = T> + 'static) -> Self {
        Self {
            driver: Box::new(driver),
        }
    }

    pub async fn recv(&mut self) -> Result<Option<T>, ProtocolError> {
        self.driver.recv().await
    }
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
pub trait ProtocolEndpointDriver: kymux_util::KySend {
    type Protocol;

    fn id(&self) -> u16;

    // object-safe
    async fn ready_boxed(self: Box<Self>) -> Result<Self::Protocol, ProtocolError>;

    // convenience helper if the concrete type is known
    async fn ready(self) -> Result<Self::Protocol, ProtocolError>
    where
        Self: Sized + 'static,
    {
        Box::new(self).ready_boxed().await
    }
}

pub struct ProtocolEndpoint<T> {
    driver: Box<dyn ProtocolEndpointDriver<Protocol = T>>,
}

impl<T> ProtocolEndpoint<T> {
    pub fn new(driver: impl ProtocolEndpointDriver<Protocol = T> + 'static) -> Self {
        Self {
            driver: Box::new(driver),
        }
    }

    pub fn id(&self) -> u16 {
        self.driver.id()
    }

    pub async fn ready(self) -> Result<T, ProtocolError> {
        self.driver.ready_boxed().await
    }
}

impl<T, D> From<D> for ProtocolEndpoint<T>
where
    D: ProtocolEndpointDriver<Protocol = T> + 'static,
{
    fn from(driver: D) -> Self {
        Self::new(driver)
    }
}

pub struct VideoServerProtocol {
    pub send: ProtocolSend<AVPacket>,
}

pub struct VideoClientProtocol {
    pub recv: ProtocolRecv<AVPacket>,
}

pub struct AudioServerProtocol {
    pub send: ProtocolSend<AVPacket>,
}

pub struct AudioClientProtocol {
    pub recv: ProtocolRecv<AVPacket>,
}

pub struct InputProtocol {
    pub send: ProtocolSend<InputPacket>,
    pub recv: ProtocolRecv<InputPacket>,
}

pub struct DataProtocol {
    pub send: ProtocolSend<DataPacket>,
    pub recv: ProtocolRecv<DataPacket>,
}

pub struct MetricsServerProtocol {
    pub send: ProtocolSend<MetricsPacket>,
}

pub struct MetricsClientProtocol {
    pub recv: ProtocolRecv<MetricsPacket>,
}

pub type VideoServerEndpoint = ProtocolEndpoint<VideoServerProtocol>;
pub type VideoClientEndpoint = ProtocolEndpoint<VideoClientProtocol>;
pub type AudioServerEndpoint = ProtocolEndpoint<AudioServerProtocol>;
pub type AudioClientEndpoint = ProtocolEndpoint<AudioClientProtocol>;
pub type InputEndpoint = ProtocolEndpoint<InputProtocol>;
pub type DataEndpoint = ProtocolEndpoint<DataProtocol>;
pub type MetricsServerEndpoint = ProtocolEndpoint<MetricsServerProtocol>;
pub type MetricsClientEndpoint = ProtocolEndpoint<MetricsClientProtocol>;
