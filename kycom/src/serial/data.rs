use super::{Deserializer, Serializer};
use async_trait::async_trait;
use bytes::BytesMut;
use kymux_types::data::*;
use std::io::{ErrorKind, Result};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub struct DataPacketSerializer;
pub struct DataPacketDeserializer;

#[async_trait]
impl Serializer for DataPacketSerializer {
    type Packet = DataPacket;

    async fn write(
        &mut self,
        packet: Self::Packet,
        writer: &mut (dyn AsyncWrite + Send + Unpin),
    ) -> Result<()> {
        let size =
            u32::try_from(packet.payload.len()).expect("Data packet size must fit in 32 bits");

        writer.write_all(&size.to_be_bytes()).await?;
        writer.write_all(&packet.payload).await?;

        Ok(())
    }
}

#[async_trait]
impl Deserializer for DataPacketDeserializer {
    type Packet = DataPacket;

    async fn read(
        &mut self,
        reader: &mut (dyn AsyncRead + Send + Unpin),
    ) -> Result<Option<Self::Packet>> {
        let mut buf = [0; 4];
        if let Err(err) = reader.read_exact(&mut buf).await {
            if err.kind() == ErrorKind::UnexpectedEof {
                return Ok(None); // EOF
            }
            return Err(err);
        }
        let size = u32::from_be_bytes(buf);

        let mut buf = BytesMut::zeroed(size as usize);
        reader.read_exact(&mut buf).await?;
        let payload = buf.freeze();

        let packet = DataPacket { payload };

        Ok(Some(packet))
    }
}
