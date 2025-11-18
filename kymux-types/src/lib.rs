pub mod av;
pub mod data;
pub mod input;
pub mod metrics;

pub use av::*;
pub use data::*;
pub use input::*;
pub use metrics::*;

use async_trait::async_trait;
use kyutil::*;
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
pub trait ProtocolEndpoint {
    type Protocol;

    fn id(&self) -> u16;
    async fn ready(self) -> Result<Self::Protocol, ProtocolError>;
}
