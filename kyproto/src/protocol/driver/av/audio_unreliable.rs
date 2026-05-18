// Project Kyber: audio_unreliable.rs
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

use crate::protocol::driver::util::seq::Sequencer;
use crate::protocol::driver::{self, av};
use crate::protocol::{ProtocolError, ProtocolRecvDriver, ProtocolSendDriver};
use crate::router::KyChannel;
use crate::runtime::{self, Instant};
use crate::task::Task;
use crate::ProtocolStats;

use std::{collections::VecDeque, time::Duration};

use async_trait::async_trait;
use byteorder::{BigEndian, ByteOrder};
use bytes::{Buf, BufMut, Bytes, BytesMut};
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
//  - end (last datagram of kypacket flag): 1 bit
//  - datagram number: 31 bits
const DATAGRAM_HEADER_SIZE: usize = 10;

pub(crate) struct AudioUnreliableProtocolSendDriver {
    ky_channel: KyChannel,
    stream: SendStream,
    kypacket_seq: u32,
}

impl AudioUnreliableProtocolSendDriver {
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

        let header = packet.header.serialize();
        let kypacket_size = header.len() + packet.payload.len();

        // TODO avoid a payload copy
        let mut data = BytesMut::with_capacity(kypacket_size);
        data.put(&header[..]);
        data.put(&packet.payload[..]);

        let mut offset = 0;
        let mut datagram_number = 0;
        while offset < data.len() {
            let payload_size = std::cmp::min(
                max_datagram_size - DATAGRAM_HEADER_SIZE,
                data.len() - offset,
            );
            let mut buf = BytesMut::with_capacity(DATAGRAM_HEADER_SIZE + payload_size);

            assert!(payload_size < 1 << 16);

            let end = offset + payload_size == data.len();
            let datagram_number_and_end = datagram_number | if end { 1 << 31 } else { 0 };

            let kypacket_seq = self.kypacket_seq;

            self.ky_channel.write_datagram_header(&mut buf);
            buf.put_u32(kypacket_seq);
            buf.put_u32(datagram_number_and_end);
            buf.put(&data[offset..offset + payload_size]);

            debug!("send datagram {kypacket_seq}:{datagram_number}");

            self.ky_channel
                .send_datagram(buf.freeze())
                .await
                .map_err(ProtocolError::new)?;

            offset += payload_size;
            datagram_number += 1;
        }

        Ok(())
    }
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ProtocolSendDriver for AudioUnreliableProtocolSendDriver {
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
    datagram_number: u32,
    end: bool,
}

pub(crate) struct AudioUnreliableProtocolRecvDriver {
    rx_client: mpsc::Receiver<AVPacket>,
    recv_stream_packets_task: Option<Task>,
    recv_datagrams_task: Option<Task>,
    process_task: Option<Task>,
}

impl AudioUnreliableProtocolRecvDriver {
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
                let ret = Self::recv_stream_packets(stream, tx2).await;
                if let Err(err) = ret {
                    error!("recv_stream_packets() error: {err}");
                }
            },
            "recv_stream_packets",
        );
        let recv_datagrams_task = Task::spawn_task(
            async move {
                let ret = Self::recv_datagrams(ky_channel, tx).await;
                if let Err(err) = ret {
                    error!("recv_datagrams() error: {err}");
                }
            },
            "recv_datagrams",
        );

        let protocol_stats = protocol_stats.clone();
        let process_task = Task::spawn_task(
            async move {
                let ret = Self::process(rx, tx_client, protocol_stats).await;
                if let Err(err) = ret {
                    error!("process() error: {err}");
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
        tx: mpsc::Sender<RecvMsg>,
    ) -> Result<(), ProtocolError> {
        loop {
            let mut seqs = [0; 4];
            stream
                .read_exact(&mut seqs)
                .await
                .map_err(ProtocolError::new)?;
            let raw_kypacket_seq = BigEndian::read_u32(&seqs);

            let packet = driver::read_packet(&mut stream, &mut AVPacketDeserializer)
                .await?
                .ok_or_else(|| ProtocolError("Missing packet data on stream".to_string()))?;

            tx.send(RecvMsg::Stream(StreamMsg {
                packet,
                raw_kypacket_seq,
            }))
            .await
            .map_err(ProtocolError::new)?;
        }
    }

    async fn recv_datagrams(
        mut ky_channel: KyChannel,
        tx: mpsc::Sender<RecvMsg>,
    ) -> Result<(), ProtocolError> {
        loop {
            let mut datagram = ky_channel
                .recv_datagram()
                .await
                .map_err(ProtocolError::new)?;
            assert!(datagram.len() >= DATAGRAM_HEADER_SIZE);
            let _endpoint_id = datagram.get_u16();
            let raw_kypacket_seq = datagram.get_u32();

            let datagram_number_and_end = datagram.get_u32();
            let datagram_number = datagram_number_and_end & ((1 << 31) - 1);
            let end = (datagram_number_and_end & (1 << 31)) != 0;

            tx.send(RecvMsg::Datagram(DatagramMsg {
                data: datagram,
                raw_kypacket_seq,
                datagram_number,
                end,
            }))
            .await
            .map_err(ProtocolError::new)?;
        }
    }

    async fn process(
        mut rx: mpsc::Receiver<RecvMsg>,
        mut tx_client: mpsc::Sender<AVPacket>,
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
                    match msg {
                        Some(RecvMsg::Stream(msg)) => {
                            if let AVPacket::Codec(packet) = &msg.packet {
                                debug!("===== SEND codec packet to client");
                                frame_size = Some(packet.header.frame_size);
                                assert!(frame_size.unwrap() > 0);
                                tx_client.send(msg.packet).await.map_err(ProtocolError::new)?;
                                continue;
                            }

                            let kypacket_seq = kypacket_sequencer.seq(msg.raw_kypacket_seq);

                            if kypacket_seq >= next_kypacket_seq {
                                pending_group.insert_stream_packet(kypacket_seq, msg.packet);
                            }
                        }
                        Some(RecvMsg::Datagram(msg)) => {
                            let kypacket_seq = kypacket_sequencer.seq(msg.raw_kypacket_seq);

                            if kypacket_seq < next_kypacket_seq {
                                debug!("===== DROP datagram {kypacket_seq}:{}", msg.datagram_number);
                                // ignore
                                continue;
                            }

                            debug!("===== RECV datagram {kypacket_seq}:{}", msg.datagram_number);
                            pending_group.insert_datagram(kypacket_seq, msg.datagram_number, msg.end, msg.data);
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

            match pending_group.take_next_packet(next_kypacket_seq) {
                Action::None => deadline = None,
                Action::Deadline(instant) => {
                    deadline = Some(instant);
                    // continue looping
                }
                Action::Packet {
                    kypacket_seq,
                    packet,
                } => {
                    assert!(frame_size.is_some());
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
                        let frame_size = frame_size.expect("No frame_size set");
                        let missing_audio_samples = missing_packets * frame_size as u64;
                        let missing_audio_samples =
                            std::cmp::min(missing_audio_samples, u32::MAX.into()) as u32;

                        let hole = AVPacket::Hole(HolePacket {
                            header: HolePacketHeader {
                                missing_audio_samples,
                            },
                        });
                        tx_client.send(hole).await.map_err(ProtocolError::new)?;
                    }

                    debug!("===== SEND kypacket {kypacket_seq} to client");
                    next_kypacket_seq = kypacket_seq + 1;
                    tx_client.send(packet).await.map_err(ProtocolError::new)?;
                    deadline = None;
                }
            }
        }
    }
}

impl Drop for AudioUnreliableProtocolRecvDriver {
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
impl ProtocolRecvDriver for AudioUnreliableProtocolRecvDriver {
    type Packet = AVPacket;

    async fn recv(&mut self) -> Result<Option<AVPacket>, ProtocolError> {
        Ok(self.rx_client.recv().await)
    }
}

#[derive(Debug)]
struct DatagramPacket {
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
    fn new() -> Self {
        Self {
            config_packet: ConfigPacket::None,
            datagrams: VecDeque::new(),
            segments: Vec::new(),
        }
    }

    fn insert_stream_packet(&mut self, kypacket_seq: u64, packet: AVPacket) {
        assert!(self.config_packet.is_none());
        self.config_packet = ConfigPacket::Ready(StreamPacket {
            packet,
            kypacket_seq,
        });
    }

    fn insert_datagram(&mut self, kypacket_seq: u64, datagram_number: u32, end: bool, data: Bytes) {
        let index = self
            .datagrams
            .binary_search_by_key(&kypacket_seq, |datagram| datagram.kypacket_seq);
        // If the kypacket having this kypacket_seq is not already re-assembled
        if index.is_err() {
            let index = self.prepare_datagram_segments(kypacket_seq);

            let is_complete = {
                let segments = &mut self.segments[index];
                segments.set_segment(datagram_number as usize, data, end);
                segments.is_complete()
            };

            if is_complete {
                let segments = self.segments.remove(index);
                let datagram_packet = segments.assemble();
                self.insert_datagram_packet(datagram_packet);
            }
        }
    }

    fn prepare_datagram_segments(&mut self, kypacket_seq: u64) -> usize {
        let index = self
            .segments
            .binary_search_by_key(&kypacket_seq, |segments| segments.kypacket_seq);
        match index {
            Ok(index) => index,
            Err(index) => {
                let segments = DatagramSegments::new(kypacket_seq);
                self.segments.insert(index, segments);
                index
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
            if let ConfigPacket::Ready(packet) = &self.config_packet {
                if packet.kypacket_seq == next_kypacket_seq {
                    // The config packet is the next expected packet
                    return NextPacket::Ready(PacketRef {
                        config_packet: true,
                    });
                }
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
                            min_instant + AudioUnreliableProtocolRecvDriver::MAX_BUFFERING;
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
    total_segments: Option<usize>, // number of segments in the kypacket
    segments: Vec<Option<Bytes>>,  // datagram i at index i
    segments_count: usize,         // number of non-None datagrams in segments
}

impl DatagramSegments {
    fn new(kypacket_seq: u64) -> Self {
        Self {
            kypacket_seq,
            total_segments: None,
            segments: Vec::new(),
            segments_count: 0,
        }
    }

    fn is_complete(&self) -> bool {
        self.total_segments == Some(self.segments_count)
    }

    fn merge_segments(segments: &Vec<Option<Bytes>>) -> Bytes {
        let mut bytes = BytesMut::new();
        for segment in segments {
            let segment = segment.as_ref().expect("Unexpected None segment");
            bytes.put_slice(segment);
        }
        bytes.freeze()
    }

    fn set_segment(&mut self, index: usize, segment: Bytes, end: bool) {
        if index >= self.segments.len() {
            self.segments.resize(index + 1, None);
        }
        if self.segments[index].is_none() {
            self.segments[index] = Some(segment);
            self.segments_count += 1;
            if end {
                assert!(self.total_segments.is_none());
                self.total_segments = Some(index + 1)
            }
        } else {
            debug!("Discarding dupliate datagram {}:{index}", self.kypacket_seq);
        }
    }

    fn assemble(self) -> DatagramPacket {
        assert!(self.is_complete());
        let data = Self::merge_segments(&self.segments);

        // TODO for now, the kypacket header is sent "as is" over datagrams.
        // In the future, they might be rewritten (we don't need the same data,
        // for example size is redundant)
        let header = MediaPacketHeader::deserialize(&data[..AVPacketHeader::SERIALIZED_SIZE]);

        let payload = data.slice(AVPacketHeader::SERIALIZED_SIZE..);
        let packet = AVPacket::Media(MediaPacket { header, payload });

        DatagramPacket {
            packet,
            kypacket_seq: self.kypacket_seq,
            instant: Instant::now(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test() {
        let d0 = {
            let mut buf = BytesMut::with_capacity(128);
            let flags = 0b101 << 61; // media + key
            buf.put_u64(0x1234567 | flags);
            buf.put_u32(17); // kypacket_size
            buf.put([1, 3, 5, 7, 9, 11, 13].as_ref()); // random data
            buf.freeze()
        };

        let d1 = Bytes::from([2, 4, 6, 8, 10, 12].as_ref());
        let d2 = Bytes::from([0xF0, 0xE0, 0xD0, 0xC0].as_ref());

        let kypacket_seq = 42;
        let mut datagram_segments = DatagramSegments::new(kypacket_seq);

        assert!(!datagram_segments.is_complete());
        datagram_segments.set_segment(1, d1, false);
        assert!(!datagram_segments.is_complete());
        datagram_segments.set_segment(2, d2, true);
        assert!(!datagram_segments.is_complete());
        datagram_segments.set_segment(0, d0, false);
        assert!(datagram_segments.is_complete());

        let datagram_packet = datagram_segments.assemble();
        assert_eq!(datagram_packet.kypacket_seq, 42);

        if let AVPacket::Media(packet) = datagram_packet.packet {
            assert_eq!(packet.header.pts, 0x1234567);
            assert!(!packet.header.is_config);
            assert!(packet.header.is_key);
            assert_eq!(packet.payload.len(), 17);

            // payload
            assert_eq!(
                &packet.payload[..],
                [1, 3, 5, 7, 9, 11, 13, 2, 4, 6, 8, 10, 12, 0xF0, 0xE0, 0xD0, 0xC0]
            );
        } else {
            panic!("Not a media packet");
        }
    }
}
