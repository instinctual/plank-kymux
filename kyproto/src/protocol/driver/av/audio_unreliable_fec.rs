// Project Kyber: audio_unreliable_fec.rs
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

use super::fec;
use crate::ProtocolStats;
use crate::protocol::driver::util::seq::Sequencer;
use crate::protocol::{ProtocolError, ProtocolRecvDriver, ProtocolSendDriver};
use crate::router::KyChannel;
use crate::runtime::{self, Instant};
use crate::task::Task;

use std::{collections::VecDeque, time::Duration};

use async_trait::async_trait;
use byteorder::{BigEndian, ByteOrder};
use bytes::{BufMut, Bytes, BytesMut};
use kymux_types::av::*;
use kymux_util::*;
use kynet::{RecvStream, SendStream};
#[allow(unused)]
use log::{debug, error, info, warn};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;

const KYPACKET_HEADER_SIZE: usize = AVPacketHeader::SERIALIZED_SIZE;

// Datagram header:
//  - endpoint id (to be written explicitly): 16 bits
//  - kypacket_seq: 32 bits
//  - RaptorQ Object Transmission Information: 96 bits
//  - RaptorQ payload id: 32 bits
const DATAGRAM_HEADER_SIZE: usize = 22;

pub(crate) struct AudioUnreliableFecProtocolSendDriver {
    ky_channel: KyChannel,
    stream: SendStream,
    kypacket_seq: u32,
}

impl AudioUnreliableFecProtocolSendDriver {
    pub(crate) async fn start(
        ky_channel: KyChannel,
        _protocol_stats: &KyArc<KyMutex<ProtocolStats>>,
    ) -> Result<Self, ProtocolError> {
        let stream = ky_channel.open_uni().await.map_err(ProtocolError::new)?;
        Ok(Self {
            ky_channel,
            stream,
            kypacket_seq: 0,
        })
    }

    async fn send_datagrams(&mut self, packet: MediaPacket) -> Result<(), ProtocolError> {
        let max_datagram_size = self
            .ky_channel
            .max_datagram_size()
            .ok_or_else(|| ProtocolError("Datagram not supported".to_string()))?;
        assert!(max_datagram_size > DATAGRAM_HEADER_SIZE);
        assert!(max_datagram_size < 0x10000);

        let max_payload_size = (max_datagram_size - DATAGRAM_HEADER_SIZE) as u16;

        let header = packet.header.serialize();
        let kypacket_size = header.len() + packet.payload.len();

        // TODO avoid a payload copy
        let mut data = BytesMut::with_capacity(kypacket_size);
        data.put(&header[..]);
        data.put(&packet.payload[..]);

        let encoder = raptorq::Encoder::with_defaults(&data, max_payload_size);
        let oti = encoder.get_config();
        let symbol_size = oti.symbol_size() as usize;

        let source_symbols = kypacket_size.div_ceil(symbol_size);

        // Add 30% repair packets (at least 2 packets)
        let repair_symbols = ((source_symbols as f32 * 0.3).ceil() as u32).max(2);

        let oti = oti.serialize();

        for encoded_packet in encoder.get_encoded_packets(repair_symbols).into_iter() {
            let kypacket_seq = self.kypacket_seq;

            let (payload_id, data) = encoded_packet.split();
            let raw_payload_id = payload_id.serialize();
            let mut buf = BytesMut::with_capacity(DATAGRAM_HEADER_SIZE + data.len());

            self.ky_channel.write_datagram_header(&mut buf);
            buf.put_u32(kypacket_seq);
            buf.put(&oti[..]);
            buf.put(&raw_payload_id[..]);
            buf.put(&data[..]);

            debug!("send datagram {kypacket_seq}:{payload_id:?}");

            self.ky_channel
                .send_datagram(buf.freeze())
                .await
                .map_err(ProtocolError::new)?;
        }

        Ok(())
    }
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ProtocolSendDriver for AudioUnreliableFecProtocolSendDriver {
    type Packet = AVPacket;

    async fn send(&mut self, packet: AVPacket) -> Result<(), ProtocolError> {
        match packet {
            AVPacket::Codec(packet) => {
                // Send over kynet stream:
                //  - kypacket_seq: 32 bits (meaningless)
                // - kypacket: 12 bytes (no payload)
                let mut buf = BytesMut::with_capacity(4 + KYPACKET_HEADER_SIZE);
                buf.put_u32(0); // meaningless
                buf.put(&packet.header.serialize()[..]);
                info!("WRITE codec packet");
                self.stream
                    .write_all(&buf)
                    .await
                    .map_err(ProtocolError::new)?;
                self.stream.flush().await.map_err(ProtocolError::new)?;
            }
            AVPacket::Media(packet) => {
                if packet.header.is_config {
                    // Send over kynet stream
                    //  - kypacket_seq: 32 bits
                    //  - group_seq: 32 bits
                    //  - kypacket: 12 bytes + payload
                    let mut buf =
                        BytesMut::with_capacity(4 + KYPACKET_HEADER_SIZE + packet.payload.len());
                    buf.put_u32(self.kypacket_seq);
                    buf.put(&packet.header.serialize()[..]);
                    buf.put(&packet.payload[..]);
                    self.stream
                        .write_all(&buf)
                        .await
                        .map_err(ProtocolError::new)?;
                    self.stream.flush().await.map_err(ProtocolError::new)?;
                } else {
                    self.send_datagrams(packet).await?;
                }

                self.kypacket_seq = self.kypacket_seq.wrapping_add(1);
            }
            AVPacket::Hole(_) => panic!("Unexpected input hole packet"),
        }

        Ok(())
    }
}

#[derive(Debug)]
enum RecvMsg {
    Stream(StreamMsg),
    Datagram(DatagramMsg),
}

#[derive(Debug)]
struct StreamMsg {
    packet: AVPacket,
    raw_kypacket_seq: u32,
}

#[derive(Debug)]
struct DatagramMsg {
    data: Bytes,
    raw_kypacket_seq: u32,
    oti: raptorq::ObjectTransmissionInformation,
    payload_id: raptorq::PayloadId,
}

impl DatagramMsg {
    fn parse(datagram: Bytes) -> Result<Self, ProtocolError> {
        let (data, oti, payload_id) = fec::parse_datagram(
            datagram.clone(),
            DATAGRAM_HEADER_SIZE,
            fec::MAX_AUDIO_PAYLOAD,
        )?;
        Ok(Self {
            raw_kypacket_seq: BigEndian::read_u32(&datagram[2..6]),
            data,
            oti,
            payload_id,
        })
    }
}

pub(crate) struct AudioUnreliableFecProtocolRecvDriver {
    rx_client: mpsc::Receiver<Result<AVPacket, ProtocolError>>,
    recv_stream_packets_task: Option<Task>,
    recv_datagrams_task: Option<Task>,
    process_task: Option<Task>,
}

impl AudioUnreliableFecProtocolRecvDriver {
    const MAX_BUFFERING: Duration = Duration::from_millis(10);

    pub(crate) async fn start(
        mut ky_channel: KyChannel,
        protocol_stats: &KyArc<KyMutex<ProtocolStats>>,
    ) -> Result<Self, ProtocolError> {
        let stream = ky_channel.accept_uni().await.map_err(ProtocolError::new)?;

        let (tx, rx) = mpsc::channel(16);
        let (tx_client, rx_client) = mpsc::channel(16);

        let tx2 = tx.clone();
        let recv_stream_packets_task = Task::spawn_task(
            async move {
                let ret = Self::recv_stream_packets(stream, tx2.clone()).await;
                if let Err(err) = ret {
                    let _ = tx2.send(Err(err)).await;
                }
            },
            "recv_stream_packets",
        );
        let recv_datagrams_task = Task::spawn_task(
            async move {
                let ret = Self::recv_datagrams(ky_channel, tx.clone()).await;
                if let Err(err) = ret {
                    let _ = tx.send(Err(err)).await;
                }
            },
            "recv_datagrams",
        );

        let protocol_stats = protocol_stats.clone();
        let process_task = Task::spawn_task(
            async move {
                let ret = Self::process(rx, tx_client.clone(), protocol_stats).await;
                if let Err(err) = ret {
                    let _ = tx_client.send(Err(err)).await;
                }
            },
            "process",
        );

        Ok(Self {
            rx_client,
            recv_stream_packets_task: Some(recv_stream_packets_task),
            recv_datagrams_task: Some(recv_datagrams_task),
            process_task: Some(process_task),
        })
    }

    async fn recv_stream_packets(
        mut stream: RecvStream,
        tx: mpsc::Sender<Result<RecvMsg, ProtocolError>>,
    ) -> Result<(), ProtocolError> {
        loop {
            let mut seqs = [0; 4];
            stream
                .read_exact(&mut seqs)
                .await
                .map_err(ProtocolError::new)?;
            let raw_kypacket_seq = BigEndian::read_u32(&seqs);

            let packet = fec::read_stream_packet(&mut stream, fec::MAX_AUDIO_PAYLOAD).await?;

            tx.send(Ok(RecvMsg::Stream(StreamMsg {
                packet,
                raw_kypacket_seq,
            })))
            .await
            .map_err(ProtocolError::new)?;
        }
    }

    async fn recv_datagrams(
        mut ky_channel: KyChannel,
        tx: mpsc::Sender<Result<RecvMsg, ProtocolError>>,
    ) -> Result<(), ProtocolError> {
        loop {
            let datagram = ky_channel
                .recv_datagram()
                .await
                .map_err(ProtocolError::new)?;
            tx.send(Ok(RecvMsg::Datagram(DatagramMsg::parse(datagram)?)))
                .await
                .map_err(ProtocolError::new)?;
        }
    }

    async fn process(
        mut rx: mpsc::Receiver<Result<RecvMsg, ProtocolError>>,
        tx_client: mpsc::Sender<Result<AVPacket, ProtocolError>>,
        protocol_stats: KyArc<KyMutex<ProtocolStats>>,
    ) -> Result<(), ProtocolError> {
        let mut kypacket_sequencer = Sequencer::<u32>::new();
        let mut pending_group = PendingGroup::new();
        let mut next_kypacket_seq = 0u64;
        let mut deadline = None;
        let mut frame_size = None;

        loop {
            tokio::select! {
                msg = rx.recv() => {
                    match msg.transpose()? {
                        Some(RecvMsg::Stream(msg)) => {
                            if let AVPacket::Codec(packet) = &msg.packet {
                                debug!("===== SEND codec packet to client");
                                frame_size = Some(packet.header.frame_size);
                                if frame_size == Some(0) { return Err(fec::invalid("zero audio frame size")); }
                                tx_client.send(Ok(msg.packet)).await.map_err(ProtocolError::new)?;
                                continue;
                            }

                            let kypacket_seq = kypacket_sequencer.seq(msg.raw_kypacket_seq);

                            if kypacket_seq >= next_kypacket_seq {
                                pending_group.insert_stream_packet(kypacket_seq, msg.packet)?;
                            }
                        }
                        Some(RecvMsg::Datagram(msg)) => {
                            let kypacket_seq = kypacket_sequencer.seq(msg.raw_kypacket_seq);

                            if kypacket_seq < next_kypacket_seq {
                                debug!("===== DROP datagram {kypacket_seq}:{:?}", msg.payload_id);
                                // ignore
                                continue;
                            }

                            debug!("===== RECV datagram {kypacket_seq}:{:?}", msg.payload_id);
                            pending_group.insert_datagram(kypacket_seq, msg.oti, msg.payload_id, msg.data)?;
                        }
                        None => return Ok(()), // No more data
                    }
                }
                Some(_) = async move {
                    match deadline {
                        Some(instant) => {
                            runtime::sleep_until(instant).await;
                            Some(())
                        },
                        None => None,
                    }
                } => {}
            }

            // Drain all completed packets before waiting for more network
            // input. In particular, a late reliable config can unlock media
            // whose datagrams have already all arrived.
            loop {
                match pending_group.take_next_packet(next_kypacket_seq) {
                    Action::None => {
                        deadline = None;
                        break;
                    }
                    Action::Deadline(instant) => {
                        deadline = Some(instant);
                        break;
                    }
                    Action::Packet {
                        kypacket_seq,
                        packet,
                    } => {
                        let frame_size =
                            frame_size.ok_or_else(|| fec::invalid("audio before codec"))?;
                        if kypacket_seq > next_kypacket_seq {
                            let missing_packets = kypacket_seq - next_kypacket_seq;
                            {
                                let mut protocol_stats = protocol_stats.lock();
                                let dropped_packets =
                                    protocol_stats.dropped_packets.unwrap_or_default();
                                protocol_stats.dropped_packets =
                                    Some(dropped_packets + missing_packets);
                            }
                            if kypacket_seq == next_kypacket_seq + 1 {
                                warn!("Missing packet {next_kypacket_seq}");
                            } else {
                                warn!(
                                    "Missing packets {next_kypacket_seq} to {}",
                                    kypacket_seq - 1
                                );
                            }

                            let missing_packets = kypacket_seq - next_kypacket_seq;
                            let missing_audio_samples =
                                missing_packets.saturating_mul(frame_size as u64);
                            let missing_audio_samples =
                                std::cmp::min(missing_audio_samples, u32::MAX.into()) as u32;

                            let hole = AVPacket::Hole(HolePacket {
                                header: HolePacketHeader {
                                    missing_audio_samples,
                                },
                            });
                            tx_client.send(Ok(hole)).await.map_err(ProtocolError::new)?;
                        }

                        debug!("===== SEND kypacket {kypacket_seq} to client");
                        next_kypacket_seq = kypacket_seq + 1;
                        tx_client
                            .send(Ok(packet))
                            .await
                            .map_err(ProtocolError::new)?;
                    }
                }
            }
        }
    }
}

impl Drop for AudioUnreliableFecProtocolRecvDriver {
    fn drop(&mut self) {
        if let Some(recv_stream_packets_task) = self.recv_stream_packets_task.take() {
            recv_stream_packets_task.cancel();
        }
        if let Some(recv_datagrams_task) = self.recv_datagrams_task.take() {
            recv_datagrams_task.cancel();
        }
        if let Some(process_task) = self.process_task.take() {
            process_task.cancel();
        }
    }
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ProtocolRecvDriver for AudioUnreliableFecProtocolRecvDriver {
    type Packet = AVPacket;

    async fn recv(&mut self) -> Result<Option<AVPacket>, ProtocolError> {
        let result = self.rx_client.recv().await.transpose();
        if result.is_err() {
            if let Some(task) = self.recv_stream_packets_task.take() {
                task.cancel();
            }
            if let Some(task) = self.recv_datagrams_task.take() {
                task.cancel();
            }
            if let Some(task) = self.process_task.take() {
                task.cancel();
            }
        }
        result
    }
}

#[derive(Debug)]
struct DatagramPacket {
    oti: raptorq::ObjectTransmissionInformation,
    packet: AVPacket,
    kypacket_seq: u64,
    instant: Instant, // assemble() timestamp
}

#[derive(Debug)]
struct StreamPacket {
    packet: AVPacket,
    kypacket_seq: u64,
}

#[derive(Debug)]
enum NextPacket {
    None,
    Ready(PacketRef),
    Deadline(Instant),
}

#[derive(Debug)]
struct PacketRef {
    config_packet: bool,
}

#[derive(Debug)]
enum Action {
    Packet { kypacket_seq: u64, packet: AVPacket },
    Deadline(Instant),
    None,
}

#[derive(Debug)]
enum ConfigPacket {
    None,
    Ready(StreamPacket),
    Sent,
}

impl ConfigPacket {
    fn is_ready(&self) -> bool {
        matches!(self, Self::Ready(_))
    }

    fn is_none(&self) -> bool {
        matches!(self, Self::None)
    }

    #[allow(dead_code)]
    fn is_sent(&self) -> bool {
        matches!(self, Self::Sent)
    }

    fn consume(&mut self) -> StreamPacket {
        assert!(self.is_ready());
        let ready_state = std::mem::replace(self, Self::Sent);
        if let Self::Ready(packet) = ready_state {
            packet
        } else {
            panic!("Attempting to consume a non-ready config packet");
        }
    }
}

#[derive(Debug)]
struct PendingGroup {
    config_packet: ConfigPacket,
    datagrams: VecDeque<DatagramPacket>,
    segments: Vec<DatagramSegments>,
}

impl PendingGroup {
    fn check_capacity(&self, additional: usize) -> Result<(), ProtocolError> {
        let mut objects = self.segments.len() + self.datagrams.len();
        let mut bytes = self
            .segments
            .iter()
            .map(|s| fec::reservation(&s.decoder.oti))
            .sum::<usize>()
            + self
                .datagrams
                .iter()
                .map(|d| fec::reservation(&d.oti))
                .sum::<usize>();
        if let ConfigPacket::Ready(StreamPacket {
            packet: AVPacket::Media(media),
            ..
        }) = &self.config_packet
        {
            objects += 1;
            bytes += media.payload.len();
        }
        fec::check_pending(objects, bytes, additional, fec::MAX_AUDIO_PAYLOAD)
    }
    fn new() -> Self {
        Self {
            config_packet: ConfigPacket::None,
            datagrams: VecDeque::new(),
            segments: Vec::new(),
        }
    }

    fn insert_stream_packet(
        &mut self,
        kypacket_seq: u64,
        packet: AVPacket,
    ) -> Result<(), ProtocolError> {
        let AVPacket::Media(media) = &packet else {
            return Err(fec::invalid("expected audio config"));
        };
        if !media.header.is_config || !self.config_packet.is_none() {
            return Err(fec::invalid("invalid or duplicate audio config"));
        }
        self.check_capacity(media.payload.len())?;
        self.config_packet = ConfigPacket::Ready(StreamPacket {
            packet,
            kypacket_seq,
        });
        Ok(())
    }

    fn insert_datagram(
        &mut self,
        kypacket_seq: u64,
        oti: raptorq::ObjectTransmissionInformation,
        payload_id: raptorq::PayloadId,
        data: Bytes,
    ) -> Result<(), ProtocolError> {
        let index = self
            .datagrams
            .binary_search_by_key(&kypacket_seq, |datagram| datagram.kypacket_seq);
        // If the kypacket having this kypacket_seq is not already re-assembled
        if index.is_err() {
            let index = self.prepare_datagram_segments(kypacket_seq, oti)?;

            let is_complete = {
                let segments = &mut self.segments[index];
                segments.add_packet(oti, payload_id, data)?;
                segments.is_complete()
            };

            if is_complete {
                let segments = self.segments.remove(index);
                let datagram_packet = segments.assemble()?;
                self.insert_datagram_packet(datagram_packet);
            }
        } else if self.datagrams[index.unwrap()].oti != oti {
            return Err(fec::invalid("OTI changed for completed audio object"));
        }
        Ok(())
    }

    fn prepare_datagram_segments(
        &mut self,
        kypacket_seq: u64,
        oti: raptorq::ObjectTransmissionInformation,
    ) -> Result<usize, ProtocolError> {
        let index = self
            .segments
            .binary_search_by_key(&kypacket_seq, |segments| segments.kypacket_seq);
        match index {
            Ok(index) => Ok(index),
            Err(index) => {
                fec::validate_oti(&oti, fec::MAX_AUDIO_PAYLOAD)?;
                self.check_capacity(fec::reservation(&oti))?;
                let segments = DatagramSegments::new(kypacket_seq, oti)?;
                self.segments.insert(index, segments);
                Ok(index)
            }
        }
    }

    fn drop_expired_segments(&mut self, until_kypacket_seq: u64) {
        let index = self
            .segments
            .binary_search_by_key(&until_kypacket_seq, |segments| segments.kypacket_seq);
        let index = index.unwrap_or_else(|index| index);
        self.segments.drain(..index);
    }

    fn insert_datagram_packet(&mut self, packet: DatagramPacket) {
        let datagram_index = self
            .datagrams
            .binary_search_by_key(&packet.kypacket_seq, |datagram| datagram.kypacket_seq);
        if let Err(datagram_index) = datagram_index {
            self.datagrams.insert(datagram_index, packet);
        }
        // else it is a duplicate, ignore
    }

    fn next_packet(&self, next_kypacket_seq: u64) -> NextPacket {
        let mut cached_min_instant = None;

        if !self.config_packet.is_none() {
            if let ConfigPacket::Ready(packet) = &self.config_packet
                && packet.kypacket_seq == next_kypacket_seq
            {
                // The config packet is the next expected packet
                return NextPacket::Ready(PacketRef {
                    config_packet: true,
                });
            }

            let datagrams = &self.datagrams;
            if !datagrams.is_empty() {
                let mut ready = false;
                if datagrams[0].kypacket_seq == next_kypacket_seq {
                    ready = true;
                } else {
                    // cached_min_instant is an Option<Option<Instant>>:
                    //  - the first Option indicates if the value is cached
                    //  - the second Option is there is a min instant at
                    //    all (if there are no items at all, there is no min)
                    let min_instant = if let Some(cached) = cached_min_instant {
                        cached
                    } else {
                        let min_instant = self.min_instant();
                        cached_min_instant = Some(min_instant);
                        min_instant
                    };
                    if let Some(min_instant) = min_instant {
                        let deadline =
                            min_instant + AudioUnreliableFecProtocolRecvDriver::MAX_BUFFERING;
                        if deadline <= Instant::now() {
                            ready = true;
                        } else {
                            return NextPacket::Deadline(deadline);
                        }
                    }
                }
                if ready {
                    // Send either the datagram or the previous config packet if not sent yet
                    return NextPacket::Ready(PacketRef {
                        config_packet: self.config_packet.is_ready(),
                    });
                }
            }
        }

        NextPacket::None
    }

    fn min_instant(&self) -> Option<Instant> {
        // Minimum of all datagrams instants
        self.datagrams.iter().map(|datagram| datagram.instant).min()
    }

    fn take_next_packet(&mut self, next_kypacket_seq: u64) -> Action {
        match self.next_packet(next_kypacket_seq) {
            NextPacket::None => Action::None,
            NextPacket::Deadline(instant) => Action::Deadline(instant),
            NextPacket::Ready(packet_ref) => {
                assert!(!self.config_packet.is_none());

                if packet_ref.config_packet {
                    assert!(self.config_packet.is_ready());
                    let config_packet = self.config_packet.consume();
                    Action::Packet {
                        kypacket_seq: config_packet.kypacket_seq,
                        packet: config_packet.packet,
                    }
                } else {
                    let datagram = self
                        .datagrams
                        .pop_front()
                        .expect("Expected a pending datagram");
                    self.drop_expired_segments(datagram.kypacket_seq);
                    Action::Packet {
                        kypacket_seq: datagram.kypacket_seq,
                        packet: datagram.packet,
                    }
                }
            }
        }
    }
}

#[derive(Debug)]
struct DatagramSegments {
    kypacket_seq: u64,
    decoder: fec::ObjectDecoder,
    assembled: Option<Vec<u8>>,
}

impl DatagramSegments {
    fn new(
        kypacket_seq: u64,
        oti: raptorq::ObjectTransmissionInformation,
    ) -> Result<Self, ProtocolError> {
        Ok(Self {
            kypacket_seq,
            decoder: fec::ObjectDecoder::new(oti, fec::MAX_AUDIO_PAYLOAD)?,
            assembled: None,
        })
    }

    fn add_packet(
        &mut self,
        oti: raptorq::ObjectTransmissionInformation,
        payload_id: raptorq::PayloadId,
        data: Bytes,
    ) -> Result<(), ProtocolError> {
        self.assembled = self.decoder.decode(oti, payload_id, data)?;
        Ok(())
    }

    fn is_complete(&self) -> bool {
        self.assembled.is_some()
    }

    fn assemble(self) -> Result<DatagramPacket, ProtocolError> {
        let data = self
            .assembled
            .ok_or_else(|| fec::invalid("incomplete audio object"))?;
        let packet = fec::media_packet(data)?;

        Ok(DatagramPacket {
            oti: self.decoder.oti,
            packet,
            kypacket_seq: self.kypacket_seq,
            instant: Instant::now(),
        })
    }
}

#[cfg(test)]
#[path = "audio_fec_validation_tests.rs"]
mod validation_tests;
