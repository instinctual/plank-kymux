use async_trait::async_trait;
use std::io::Result;
use tokio::io::{AsyncRead, AsyncWrite};

pub mod av;
pub mod data;
pub mod input;
pub mod metrics;

#[async_trait]
pub trait Serializer {
    type Packet;
    async fn write(
        &mut self,
        packet: Self::Packet,
        writer: &mut (dyn AsyncWrite + Send + Unpin),
    ) -> Result<()>;
}

#[async_trait]
pub trait Deserializer {
    type Packet;
    async fn read(
        &mut self,
        reader: &mut (dyn AsyncRead + Send + Unpin),
    ) -> Result<Option<Self::Packet>>;
}
