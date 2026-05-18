// Project Kyber: video_gopstream.rs
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

//! This protocol opens a new QUIC stream for every GOP (group of pictures).
//!
//! Concretely, a new QUIC stream is started on every keyframe.
//!
//! The principle is to transmit a GOP reliably (with retransmissions when
//! packets are lost, like TCP), but abandon any old GOPs whenever a new one is
//! started.
//!
//! The current codec and config packet are repeated at the beginning of each
//! QUIC stream, with an identifier so that the receiver can determine if a new
//! one should be sent to the decoder.
//!
//! For example, if the producer sends these packets in sequence:
//!  - codec1 (a codec packet)
//!  - cfg1 (a config packet)
//!  - a sequence of media packets: I P P P P P P I P P P
//!  - cfg2
//!  - a sequence of media packets: I P P P P P P
//!  - codec2
//!  - cfg3
//!  - a sequence of media packets: I P P P I P P P P
//!
//! (I: key frame, P: non-key frame)
//!
//! Then the kymux server will transmit these data to the kymux client as
//! follow:
//!
//! QUIC stream 1:
//!     +-----+--------+------+---------------+
//!     | ids | codec1 | cfg1 | I P P P P P P |
//!     +-----+--------+------+---------------+
//! QUIC stream 2:
//!     +-----+--------+------+-----------+
//!     | ids | codec1 | cfg1 | I P P P P |
//!     +-----+--------+------+-----------+
//! QUIC stream 3:
//!     +-----+--------+------+-------------+
//!     | ids | codec1 | cfg2 | I P P P P P |
//!     +-----+--------+------+-------------+
//! QUIC stream 4:
//!     +-----+--------+------+---------+
//!     | ids | codec2 | cfg3 | I P P P |
//!     +-----+--------+------+---------+
//! QUIC stream 5:
//!     +-----+--------+------+-----------+
//!     | ids | codec2 | cfg3 | I P P P P |
//!     +-----+--------+------+-----------+
//!
//! The ids are three 32-bit numbers:
//!  - the gop id, to make sure we consider GOPs in order
//!  - the associated codec number
//!  - the associated config number
//!
//! The gop id is incremented on every new keyframe. The other numbers are
//! incremented every time a new codec or config packet (respectively) is
//! received from the producer.
//!
//! The receiver uses these ids to make sure it sends a single codec or config
//! packet at most once.
//!
//! Every time a new QUIC stream is opened by the server, the previous one is
//! reset, so no more retransmissions will occur for the old GOPs.

use crate::protocol::driver::{self, av};
use crate::protocol::{ProtocolError, ProtocolRecvDriver, ProtocolSendDriver};
use crate::router::KyChannel;
use crate::runtime;
use crate::task::Task;
use crate::ProtocolStats;

use async_trait::async_trait;
use byteorder::{BigEndian, ByteOrder, WriteBytesExt};
use kymux_types::av::*;
use kymux_util::*;
use kynet::error::ConnectionError;
use kynet::{RecvStream, SendStream};
#[allow(unused)]
use log::{debug, error, info, warn};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;

pub(crate) struct VideoGopStreamProtocolSendDriver {
    ky_channel: KyChannel,
    current_stream: Option<SendStream>,
    gop_id: u32,
    codec_gen: u32,
    codec_packet: Option<CodecPacket>,
    config_gen: u32,
    config_packet: Option<MediaPacket>,
}

impl VideoGopStreamProtocolSendDriver {
    pub(crate) async fn start(
        ky_channel: KyChannel,
        _protocol_stats: &KyArc<KyMutex<ProtocolStats>>,
    ) -> Result<Self, ProtocolError> {
        Ok(Self {
            ky_channel,
            current_stream: None,
            gop_id: 0,
            codec_gen: 0,
            codec_packet: None,
            config_gen: 0,
            config_packet: None,
        })
    }
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ProtocolSendDriver for VideoGopStreamProtocolSendDriver {
    type Packet = AVPacket;

    async fn send(&mut self, packet: AVPacket) -> Result<(), ProtocolError> {
        match packet {
            AVPacket::Codec(packet) => {
                self.codec_gen += 1;
                self.codec_packet = Some(packet);
            }
            AVPacket::Media(packet) => {
                if packet.header.is_config {
                    self.config_gen += 1;
                    self.config_packet = Some(packet);
                } else {
                    if packet.header.is_key {
                        // Start a new stream
                        let mut new_stream = self
                            .ky_channel
                            .open_uni()
                            .await
                            .map_err(ProtocolError::new)?;

                        // First write the current codec and config packets
                        // numbers. Since they are repeated on each stream,
                        // this helps the receiver to determine when there is a
                        // real new codec or config packet.
                        let mut buf = vec![];
                        AsyncWriteExt::write_u32(&mut buf, self.gop_id);
                        AsyncWriteExt::write_u32(&mut buf, self.codec_gen);
                        AsyncWriteExt::write_u32(&mut buf, self.config_gen);

                        self.gop_id += 1;

                        let codec_packet = &self.codec_packet.as_ref().unwrap();
                        let config_packet = &self.config_packet.as_ref().unwrap();

                        buf.extend_from_slice(&codec_packet.header.serialize());
                        buf.extend_from_slice(&config_packet.header.serialize());
                        buf.extend_from_slice(&config_packet.payload);

                        new_stream
                            .write_all(&buf)
                            .await
                            .map_err(ProtocolError::new)?;
                        new_stream.flush().await.map_err(ProtocolError::new)?;

                        if let Some(mut old_stream) = self.current_stream.replace(new_stream) {
                            if let Err(err) = old_stream.finish().await {
                                warn!("Could not close stream: {err}");
                            }
                        }
                    }

                    if let Some(stream) = &mut self.current_stream {
                        stream
                            .write_all(&packet.header.serialize())
                            .await
                            .map_err(ProtocolError::new)?;
                        stream
                            .write_all(&packet.payload)
                            .await
                            .map_err(ProtocolError::new)?;
                        stream.flush().await.map_err(ProtocolError::new)?;
                    } else {
                        warn!(
                            "Unexpected missing current stream, a key packet has not been received"
                        );
                    }
                }
            }
            AVPacket::Hole(_) => panic!("Unexpected input hole packet"),
        }
        Ok(())
    }
}

#[derive(Debug)]
struct Gop {
    id: u32,
    codec_gen: u32,
    config_gen: u32,
}

#[derive(Debug)]
enum RecvMsg {
    NewStream(RecvStream),
    StreamIds { recv: RecvStream, gop: Gop },
    NewPacket { packet: AVPacket, gop_id: u32 },
}

pub(crate) struct VideoGopStreamProtocolRecvDriver {
    tx: mpsc::Sender<RecvMsg>,
    rx: mpsc::Receiver<RecvMsg>,
    gop: Option<Gop>,
    last_codec_gen: u32,
    last_config_gen: u32,
    accept_unis_task: Option<Task>,
    read_packets_task: Option<Task>,
}

impl VideoGopStreamProtocolRecvDriver {
    pub(crate) async fn start(
        ky_channel: KyChannel,
        _protocol_stats: &KyArc<KyMutex<ProtocolStats>>,
    ) -> Result<Self, ProtocolError> {
        let (tx, rx) = mpsc::channel(16);

        let tx2 = tx.clone();
        let accept_unis_task = Task::spawn_task(
            async move {
                let ret = Self::accept_unis(ky_channel, tx2).await;
                if let Err(err) = ret {
                    error!("accept_uni_streams() error: {err}");
                }
            },
            "accept_unis",
        );

        Ok(Self {
            tx,
            rx,
            gop: None,
            last_codec_gen: 0,
            last_config_gen: 0,
            accept_unis_task: Some(accept_unis_task),
            read_packets_task: None,
        })
    }

    async fn accept_unis(
        mut ky_channel: KyChannel,
        tx: mpsc::Sender<RecvMsg>,
    ) -> Result<(), ProtocolError> {
        loop {
            let recv = ky_channel.accept_uni().await.map_err(ProtocolError::new)?;
            tx.send(RecvMsg::NewStream(recv))
                .await
                .map_err(ProtocolError::new)?;
        }
    }

    async fn read_stream_ids(
        mut recv: RecvStream,
        tx: mpsc::Sender<RecvMsg>,
    ) -> Result<(), ProtocolError> {
        let mut ids = [0; 12];
        recv.read_exact(&mut ids)
            .await
            .map_err(ProtocolError::new)?;
        let id = BigEndian::read_u32(&ids[..4]);
        let codec_gen = BigEndian::read_u32(&ids[4..8]);
        let config_gen = BigEndian::read_u32(&ids[8..]);
        tx.send(RecvMsg::StreamIds {
            recv,
            gop: Gop {
                id,
                codec_gen,
                config_gen,
            },
        })
        .await
        .map_err(ProtocolError::new)?;
        Ok(())
    }

    async fn read_packets(
        mut recv: RecvStream,
        tx: mpsc::Sender<RecvMsg>,
        gop_id: u32,
    ) -> Result<(), ProtocolError> {
        while let Some(packet) = driver::read_packet(&mut recv, &mut AVPacketDeserializer).await? {
            tx.send(RecvMsg::NewPacket { packet, gop_id })
                .await
                .map_err(ProtocolError::new)
                .map_err(ProtocolError::new)?;
        }
        Ok(())
    }
}

impl Drop for VideoGopStreamProtocolRecvDriver {
    fn drop(&mut self) {
        if let Some(accept_unis_task) = self.accept_unis_task.take() {
            accept_unis_task.cancel();
        }
        if let Some(read_packets_task) = self.read_packets_task.take() {
            read_packets_task.cancel();
        }
    }
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ProtocolRecvDriver for VideoGopStreamProtocolRecvDriver {
    type Packet = AVPacket;

    async fn recv(&mut self) -> Result<Option<AVPacket>, ProtocolError> {
        while let Some(msg) = self.rx.recv().await {
            match msg {
                RecvMsg::NewStream(recv) => {
                    info!("NewStream");
                    let tx = self.tx.clone();

                    runtime::spawn(async move {
                        let ret = Self::read_stream_ids(recv, tx).await;
                        if let Err(err) = ret {
                            error!("read_stream_ids() error: {err}");
                        }
                    });
                }
                RecvMsg::StreamIds { recv, gop } => {
                    let is_newer = self
                        .gop
                        .as_ref()
                        .map(|prev| gop.id > prev.id)
                        .unwrap_or(true);
                    if is_newer {
                        let gop_id = gop.id;
                        self.gop = Some(gop);
                        if let Some(task) = self.read_packets_task.take() {
                            // This is not totally optimal: we should abandon an
                            // old stream only once we received a complete frame on
                            // the new stream (in case we receive packets on an old
                            // stream before we receive a full frame on the new
                            // stream), but this is way more complex, so keep it
                            // simple: only keep the latest QUIC stream.
                            task.cancel();
                        }

                        let tx = self.tx.clone();
                        let read_packets_task = Task::spawn_task(
                            async move {
                                let ret = Self::read_packets(recv, tx, gop_id).await;
                                if let Err(err) = ret {
                                    error!("read_packets() error: {err}");
                                }
                            },
                            "read_packets",
                        );

                        self.read_packets_task = Some(read_packets_task);
                    }
                }
                RecvMsg::NewPacket { packet, gop_id } => {
                    let gop = self.gop.as_ref().unwrap();
                    if gop_id == gop.id {
                        match packet {
                            AVPacket::Codec(_) => {
                                if gop.codec_gen == self.last_codec_gen {
                                    // Ignore
                                    continue;
                                }

                                self.last_codec_gen = gop.codec_gen;
                            }
                            AVPacket::Media(ref packet) => {
                                if packet.header.is_config {
                                    if gop.config_gen == self.last_config_gen {
                                        // Ignore
                                        continue;
                                    }

                                    self.last_config_gen = gop.config_gen;
                                }
                            }
                            AVPacket::Hole(_) => panic!("Unexpected input hole packet"),
                        }

                        return Ok(Some(packet));
                    }
                }
            }
        }

        Ok(None)
    }
}
