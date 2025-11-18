use crate::protocol::driver::av;
use crate::protocol::{ProtocolError, ProtocolRecvDriver, ProtocolSendDriver};
use crate::router::KyChannel;
use crate::ProtocolStats;

use async_trait::async_trait;
use bytes::BytesMut;
use kymux_types::av::*;
use kynet::error::ReadExactError;
use kynet::{RecvStream, SendStream};
use kyutil::*;

pub(crate) struct ReliableProtocolSendDriver {
    ky_channel: KyChannel,
    send: SendStream,
}

impl ReliableProtocolSendDriver {
    pub(crate) async fn start(
        ky_channel: KyChannel,
        _protocol_stats: &KyArc<KyMutex<ProtocolStats>>,
    ) -> Result<Self, ProtocolError> {
        let send = ky_channel.open_uni().await.map_err(ProtocolError::new)?;
        Ok(Self { ky_channel, send })
    }
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ProtocolSendDriver for ReliableProtocolSendDriver {
    type Packet = AVPacket;

    async fn send(&mut self, packet: AVPacket) -> Result<(), ProtocolError> {
        match packet {
            AVPacket::Codec(packet) => {
                let header = packet.header.serialize();
                self.send
                    .write_all(&header)
                    .await
                    .map_err(ProtocolError::new)?;
            }
            AVPacket::Media(packet) => {
                let header = packet.header.serialize();
                self.send
                    .write_all(&header)
                    .await
                    .map_err(ProtocolError::new)?;
                self.send
                    .write_all(&packet.payload)
                    .await
                    .map_err(ProtocolError::new)?;
            }
            AVPacket::Hole(_) => panic!("Unexpected input hole packet"),
        }

        Ok(())
    }
}

pub(crate) struct ReliableProtocolRecvDriver {
    ky_channel: KyChannel,
    recv: RecvStream,
}

impl ReliableProtocolRecvDriver {
    pub(crate) async fn start(
        mut ky_channel: KyChannel,
        _protocol_stats: &KyArc<KyMutex<ProtocolStats>>,
    ) -> Result<Self, ProtocolError> {
        let recv = ky_channel.accept_uni().await.map_err(ProtocolError::new)?;
        Ok(Self { ky_channel, recv })
    }
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ProtocolRecvDriver for ReliableProtocolRecvDriver {
    type Packet = AVPacket;

    async fn recv(&mut self) -> Result<Option<AVPacket>, ProtocolError> {
        av::read_packet(&mut self.recv).await
    }
}
