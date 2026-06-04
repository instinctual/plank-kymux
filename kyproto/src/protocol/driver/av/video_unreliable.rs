// Project Kyber: video_unreliable.rs
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

//! This protocol, tailored for video streaming, transmits config packets
//! (SPS/PPS) over a kynet stream (QUIC or WebTransport stream), and media data
//! over kynet datagrams (QUIC or WebTransport datagrams).
//!
//! The _sender_ receives packets from a client. It sends to the other kyproto
//! peer the initial codec packet and all config packets (SPS/PPS) over a single
//! kynet stream, in sequence. It splits media packets into segments so that it
//! can send datagrams having size max_datagram_size() (exposed by kynet),
//! including some additional headers.
//!
//! The _receiver_ listens for 3 events:
//!  - accept a kynet uni-stream
//!  - receive kypackets over a previously accepted kynet uni-stream
//!  - receive datagrams
//!
//! and exposes them in a single queue (MPSC).
//!
//! It then re-assembles datagrams and reorders kypackets, and sends them to the
//! receiving client. Since datagrams may be reordered or lost, it bufferizes
//! when necessary to wait some time for missing packets, but after some timeout
//! it considers a packet as lost, and continue sending the following ones.
//!
//!
//! ## Protocol and implementation details
//!
//! ### Sender
//!
//! The Kymux sender receives 3 types of packets from the client:
//!  - `AVPacket::Codec(packet)`: a codec packet (it indicates the codec used),
//!    it is assumed that a codec packet is sent exactly once, as the first
//!    packet of the stream. No more codec packets should be sent.
//!  - `AVPacket::Media(packet)` if `packet.header.is_config`: a config packet
//!    (SPS/PPS).
//!  - `AVPacket::Media(packet)` if `!packet.header.is_config`: a media packet.
//!
//! It initially opens a single unidirectional kynet stream to the kynet
//! receiver. It sends the initial codec packet and config packets over this
//! kynet stream in sequence, prefixed with a small header containing sequence
//! numbers (described later). On the wire, each packet has the following
//! format:
//!
//!  - kypacket_seq: 32 bits
//!  - group_seq: 32 bits
//!  - kypacket headers: 12 bytes
//!  - kypacket payload: (payload length)
//!
//! The sequence numbers are set to 0 for codec packets, since they are
//! meaningless here.
//!
//! Media packets (where `is_config` is false) are split into segments to be
//! sent in kynet datagrams:
//!
//! ```notrust
//!                    +-------------------------+ +----+ +------------------+
//!      video packets |            P0           | | P1 | |       P2         |
//!                    +-------------------------+ +----+ +------------------+
//!            | split
//!            v
//!                    +----+ +----+ +----+ +----+ +----+ +----+ +----+ +----+
//!    kynet datagrams |P0D0| |P0D1| |P0D2| |P0D3| |P1D0| |P2D0| |P2D1| |P2D2|
//!                    +----+ +----+ +----+ +---#+ +---#+ +----+ +----+ +---#+
//!                                             ^      ^                    ^
//!                                             |      |                    |
//!                                               end of video packet flag
//! ```
//!
//! Here is the format of the kynet datagram payload:
//!
//!  - endpoint id: 16 bits (necessary for routing)
//!  - kypacket_seq: 32 bits
//!  - group_seq: 32 bits
//!  - end flag: 1 bit
//!  - datagram number: 31 bits
//!  - kypacket segment: chunk of kypacket "as is" (the first one includes the
//!    kypacket header)
//!
//!
//! ### Sequence numbers
//!
//! The `kypacket_seq` sequence number is incremented for each kypacket (config
//! packet or not), and wraps around (see [Sequencer] documentation to know how
//! overflow is handled by the receiver side). It allows to detect missing
//! packets and reorder them.
//!
//! `group_seq` is an additional sequence number incremented on each config
//! packet. Its purpose is to group packets in relation to a config packet. A
//! media packet is only meaningful after its previous config packet (it may
//! reference SPS/PPS). Concretely, this allows to ignore packets associated to
//! a config packet which is not received yet.
//!
//!
//! ### Receiver
//!
//! The receiver aggregates events from different sources (uni-stream opening,
//! kypackets over QUIC streams, QUIC datagrams) into an intermediate queue of
//! re-constructed packets (stream packets and datagram messages).
//!
//! In parallel, it consumes this queue (it may also be waken up by a timeout)
//! to send packets to a client queue, ready to be consumed by the kyproto
//! client. Some packets may be missing (if not received on time).
//!
//! The received packets are stored in a way which facilitates processing and
//! decision to send data to the client. Kynet datagrams pass through an
//! additional layer to be reordered and re-assembled.
//!
//! ```notrust
//!          QUIC stream         QUIC datagrams
//!              |                     |
//!              |                     v
//!              |           +------------------+
//!              |           | DatagramSegments |
//!              |           +------------------+
//!              |                     |
//!              | StreamPacket        | DatagramPacket
//!              v                     v
//!           +---------------------------+
//!           |       PendingGroups       |
//!           +---------------------------+
//! ```
//!
//! This first layer is responsible to re-assemble kypackets from datagram
//! segments. Concretly, a struct `DatagramSegments` keeps all the received
//! segments, in order, for a given `kypacket_seq` until it is complete:
//!
//!
//! ```notrust
//!                    DatagramSegments
//!                    +----+        +----+ +----+        +----+
//!     kypacket 37:   | D0 |  None  | D2 | | D3 |  None  | D5 |  (total=6)
//!                    +----+        +----+ +----+        +---#+
//!                                                           ^ end of packet
//!                           +----+ +----+
//!     kypacket 39:    None  | D1 | | D2 |  (total=?)
//!                           +----+ +----+
//!
//!                    +----+
//!     kypacket 42:   | D0 |  (total=1)   // ready to be assembled
//!                    +---#+
//!                        ^ end of packet
//! ```
//!
//! Once complete, this layer produces a full kypacket (`DatagramPacket`), via
//! the method [`DatagramSegments::assemble()`].
//!
//! The packets received from the kynet stream or reassembled from kynet
//! datagrams are stored in a [`PendingGroup`] associated with their
//! `group_seq`. This struct embeds:
//!   - the config packet for this group (if already received)
//!   - the pending kypackets already re-assembled from datagrams
//!   - the pending datagram segments (parts of datagram packets not
//!     re-assembled yet)
//!
//! Here is a schema for illustrating the config packet and the kypackets
//! reassembled from datagrams (the pending datagram segments are described
//! above) associated to groups:
//!
//! ```notrust
//!             config packet    pending
//!               (SPS/PPS)      media packets
//!              +---------+           +---+       +---+ +---+             +---+
//!     group 0: | CFG P00 |      ___  |P01|  ___  |P03| |P04|  ___   ___  |P07|
//!              +---------+           +---+       +---+ +---+             +---+
//!                              +---+       +---+       +---+
//!     group 1:  _________      |P09|  ___  |P11|  ___  |P13|
//!                              +---+       +---+       +---+
//!
//!     group 2:  ...
//! ```
//!
//! ### Processing
//!
//! When a new event occurs (new kypacket over a kynet stream, new datagram or
//! deadline reached), the receiver analyzes the current state to decide whether
//! to send new packets to the client.
//!
//! Here is an overview of the strategy to find the next packet to send (see
//! [`Forwarder::next_packet()`]).
//!
//! Firstly, the obvious case, if the packet with the expected next
//! `kypacket_seq` is available (whether it's a config packet or not), it is
//! sent immediately.
//!
//! The interesting case is when a packet is available, but not the next one (it
//! has a higher `kypacket_seq`), meaning that the packets to send before are
//! not available yet. Clearly, we want to bufferize a bit to compensate for
//! packet reordering, but we don't want to wait indefinitely because the packet
//! may be lost.
//!
//! Also, it makes sense to "drop" (i.e. not wait for) "lost" packets only if
//! the next available packet may contribute to a frame. For example, if the
//! receiver only knows the next config packet but has no media packet depending
//! on it, it should not send it immediately, so that if it receives missing
//! packets from a previous group beforehand, it has a chance to transmit them
//! to the client instead of dropping them.
//!
//! The obvious solution is to send the next available packet after some
//! arbitrary timeout ([`Forwarder::MAX_BUFFERING`] currently set to 50ms). The
//! less obvious problem is to decide when to start this timeout (i.e. from
//! which starting point should the deadline be computed).
//!
//! We could consider starting the timeout when this available packet is
//! received, but this is a poor choice: the packets may be received out of
//! order, and some more recent packets may have already been received. With
//! this strategy, receiving an additional packet may delay the time the
//! receiver would forward data to the client, which is undesirable.
//!
//! Therefore, the strategy implemented in this protocol is to use the
//! assemble() date of the oldest re-assembled datagram packet. In other words,
//! for each kypacket, the receiver stores the reception date of the last
//! datagram that completed it; over all re-assembled kypackets not sent (or
//! dropped) yet, it considers the minimum (it received a full kypacket at this
//! date, so the next kypacket was expected to be received at least as early),
//! and adds a buffering delay (20ms). This is the new deadline to send the next
//! packet (that may be immediately).
//!
//! There is no need to timestamp config packets (i.e. kynet stream packets): as
//! mentioned earlier, a config packet will never be sent alone on a timeout
//! basis (at least one datagram packet from the same group must be available).
//!
//! Note: Currently, the implementation does not take keyframes into account. It
//! may be interesting to add yet another sequence level (gop_seq) to send media
//! packets only after the GOP keyframe is sent. It will depend if we use an
//! intra-refresh strategy or if we send keyframes often.

use crate::protocol::driver::av;
use crate::protocol::driver::util::seq::Sequencer;
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
use tokio::sync::mpsc;

const KYPACKET_HEADER_SIZE: usize = AVPacketHeader::SERIALIZED_SIZE;

// Datagram header:
//  - endpoint id (to be written explicitly): 16 bits
//  - kypacket_seq: 32 bits
//  - group_seq: 32 bits (incremented on each config packet)
//  - end (last datagram of kypacket flag): 1 bit
//  - datagram number: 31 bits
const DATAGRAM_HEADER_SIZE: usize = 14;

pub(crate) struct VideoUnreliableProtocolSendDriver {
    ky_channel: KyChannel,
    stream: SendStream,
    kypacket_seq: u32,
    group_seq: u32,
}

impl VideoUnreliableProtocolSendDriver {
    pub(crate) async fn start(
        ky_channel: KyChannel,
        _protocol_stats: &KyArc<KyMutex<ProtocolStats>>,
    ) -> Result<Self, ProtocolError> {
        let stream = ky_channel.open_uni().await.map_err(ProtocolError::new)?;
        Ok(Self {
            ky_channel,
            stream,
            kypacket_seq: 0,
            group_seq: 0,
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
            let group_seq = self.group_seq;

            self.ky_channel.write_datagram_header(&mut buf);
            buf.put_u32(kypacket_seq);
            buf.put_u32(group_seq);
            buf.put_u32(datagram_number_and_end);
            buf.put(&data[offset..offset + payload_size]);

            debug!("send datagram {kypacket_seq}:{datagram_number} (group={group_seq:?})");

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
impl ProtocolSendDriver for VideoUnreliableProtocolSendDriver {
    type Packet = AVPacket;

    async fn send(&mut self, packet: AVPacket) -> Result<(), ProtocolError> {
        match packet {
            AVPacket::Codec(packet) => {
                // Send over kynet stream:
                //  - kypacket_seq: 32 bits (meaningless)
                //  - group_seq: 32 bits (meaningless)
                // - kypacket: 12 bytes (no payload)
                let mut buf = BytesMut::with_capacity(8 + KYPACKET_HEADER_SIZE);
                buf.put_u64(0); // meaningless
                buf.put(&packet.header.serialize()[..]);
                info!("WRITE codec packet");
                self.stream
                    .write_all(&buf)
                    .await
                    .map_err(ProtocolError::new)?;
            }
            AVPacket::Media(packet) => {
                if packet.header.is_config {
                    // Send over kynet stream
                    //  - kypacket_seq: 32 bits
                    //  - group_seq: 32 bits
                    //  - kypacket: 12 bytes + payload
                    self.group_seq = self.group_seq.wrapping_add(1);
                    let mut buf =
                        BytesMut::with_capacity(8 + KYPACKET_HEADER_SIZE + packet.payload.len());
                    buf.put_u32(self.kypacket_seq);
                    buf.put_u32(self.group_seq);
                    buf.put(&packet.header.serialize()[..]);
                    buf.put(&packet.payload[..]);
                    self.stream
                        .write_all(&buf)
                        .await
                        .map_err(ProtocolError::new)?;
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
    raw_group_seq: u32,
}

#[derive(Debug)]
struct DatagramMsg {
    data: Bytes,
    raw_kypacket_seq: u32,
    raw_group_seq: u32,
    datagram_number: u32,
    end: bool,
}

pub(crate) struct VideoUnreliableProtocolRecvDriver {
    rx_client: mpsc::Receiver<AVPacket>,
    recv_stream_packets_task: Option<Task>,
    recv_datagrams_task: Option<Task>,
    process_task: Option<Task>,
}

impl VideoUnreliableProtocolRecvDriver {
    const MAX_BUFFERING: Duration = Duration::from_millis(50);

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
            let mut seqs = [0; 8];
            stream
                .read_exact(&mut seqs)
                .await
                .map_err(ProtocolError::new)?;
            let raw_kypacket_seq = BigEndian::read_u32(&seqs[..4]);
            let raw_group_seq = BigEndian::read_u32(&seqs[4..]);

            let packet = av::read_packet(&mut stream)
                .await?
                .ok_or_else(|| ProtocolError("Missing packet data on stream".to_string()))?;

            tx.send(RecvMsg::Stream(StreamMsg {
                packet,
                raw_kypacket_seq,
                raw_group_seq,
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
            let raw_group_seq = datagram.get_u32();

            let datagram_number_and_end = datagram.get_u32();
            let datagram_number = datagram_number_and_end & ((1 << 31) - 1);
            let end = (datagram_number_and_end & (1 << 31)) != 0;

            tx.send(RecvMsg::Datagram(DatagramMsg {
                data: datagram,
                raw_kypacket_seq,
                raw_group_seq,
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
        let mut group_sequencer = Sequencer::<u32>::new();
        let mut kypacket_sequencer = Sequencer::<u32>::new();
        let mut pending_groups = PendingGroups::new();
        let mut next_kypacket_seq = 0u64;
        let mut deadline = None;

        loop {
            tokio::select! {
                msg = rx.recv() => {
                    match msg {
                        Some(RecvMsg::Stream(msg)) => {
                            if let AVPacket::Codec(packet) = &msg.packet {
                                debug!("===== SEND codec packet to client");
                                tx_client.send(msg.packet).await.map_err(ProtocolError::new)?;
                                continue;
                            }

                            let kypacket_seq = kypacket_sequencer.seq(msg.raw_kypacket_seq);
                            let group_seq = group_sequencer.seq(msg.raw_group_seq);

                            if kypacket_seq >= next_kypacket_seq {
                                pending_groups.insert_stream_packet(group_seq, kypacket_seq, msg.packet);
                            }
                        }
                        Some(RecvMsg::Datagram(msg)) => {
                            let kypacket_seq = kypacket_sequencer.seq(msg.raw_kypacket_seq);
                            let group_seq = group_sequencer.seq(msg.raw_group_seq);

                            if kypacket_seq < next_kypacket_seq {
                                debug!("===== DROP datagram {kypacket_seq}:{}", msg.datagram_number);
                                // ignore
                                continue;
                            }

                            debug!("===== RECV datagram {kypacket_seq}:{}", msg.datagram_number);
                            pending_groups.insert_datagram(group_seq, kypacket_seq, msg.datagram_number, msg.end, msg.data);
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

            match pending_groups.take_next_packet(next_kypacket_seq) {
                Action::None => deadline = None,
                Action::Deadline(instant) => {
                    deadline = Some(instant);
                    // continue looping
                }
                Action::Packet {
                    kypacket_seq,
                    packet,
                } => {
                    debug!("===== SEND kypacket {kypacket_seq} to client");
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
                    }
                    next_kypacket_seq = kypacket_seq + 1;
                    tx_client.send(packet).await.map_err(ProtocolError::new)?;
                    deadline = None;
                }
            }
        }
    }
}

impl Drop for VideoUnreliableProtocolRecvDriver {
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
impl ProtocolRecvDriver for VideoUnreliableProtocolRecvDriver {
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
    pending_group_index: usize,
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
struct PendingGroups {
    pending_groups: Vec<PendingGroup>,
}

impl PendingGroups {
    fn new() -> Self {
        Self {
            pending_groups: Vec::new(),
        }
    }

    fn prepare_pending_group(&mut self, group_seq: u64) -> usize {
        let index = self
            .pending_groups
            .binary_search_by_key(&group_seq, |pending_group| pending_group.group_seq);

        match index {
            Ok(index) => index,
            Err(index) => {
                let pending_group = PendingGroup::new(group_seq);
                self.pending_groups.insert(index, pending_group);
                index
            }
        }
    }

    fn insert_stream_packet(&mut self, group_seq: u64, kypacket_seq: u64, packet: AVPacket) {
        let index = self.prepare_pending_group(group_seq);

        let pending_group = &mut self.pending_groups[index];
        assert!(pending_group.config_packet.is_none());
        pending_group.config_packet = ConfigPacket::Ready(StreamPacket {
            packet,
            kypacket_seq,
        });
    }

    fn insert_datagram(
        &mut self,
        group_seq: u64,
        kypacket_seq: u64,
        datagram_number: u32,
        end: bool,
        data: Bytes,
    ) {
        let index = self.prepare_pending_group(group_seq);

        let pending_group = &mut self.pending_groups[index];
        pending_group.insert_datagram(kypacket_seq, datagram_number, end, data);
    }

    fn next_packet(&self, next_kypacket_seq: u64) -> NextPacket {
        let mut cached_min_instant = None;

        for (pending_group_index, pending_group) in self.pending_groups.iter().enumerate() {
            // Ignore pending groups without config packet
            if !pending_group.config_packet.is_none() {
                if let ConfigPacket::Ready(packet) = &pending_group.config_packet {
                    if packet.kypacket_seq == next_kypacket_seq {
                        // The config packet is the next expected packet
                        return NextPacket::Ready(PacketRef {
                            pending_group_index,
                            config_packet: true,
                        });
                    }
                }

                let datagrams = &pending_group.datagrams;
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
                                min_instant + VideoUnreliableProtocolRecvDriver::MAX_BUFFERING;
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
                            pending_group_index,
                            config_packet: pending_group.config_packet.is_ready(),
                        });
                    }
                }
            }
        }

        NextPacket::None
    }

    fn min_instant(&self) -> Option<Instant> {
        // Minimum of all datagrams instants across all pending groups
        self.pending_groups
            .iter()
            .filter_map(|pending_group| {
                pending_group
                    .datagrams
                    .iter()
                    .map(|datagram| datagram.instant)
                    .min()
            })
            .min()
    }

    fn take_next_packet(&mut self, next_kypacket_seq: u64) -> Action {
        match self.next_packet(next_kypacket_seq) {
            NextPacket::None => Action::None,
            NextPacket::Deadline(instant) => Action::Deadline(instant),
            NextPacket::Ready(packet_ref) => {
                if packet_ref.pending_group_index > 0 {
                    self.pending_groups.drain(..packet_ref.pending_group_index);
                }

                let pending_group = &mut self.pending_groups[0];

                assert!(!pending_group.config_packet.is_none());

                if packet_ref.config_packet {
                    assert!(pending_group.config_packet.is_ready());
                    let config_packet = pending_group.config_packet.consume();
                    Action::Packet {
                        kypacket_seq: config_packet.kypacket_seq,
                        packet: config_packet.packet,
                    }
                } else {
                    let datagram = pending_group
                        .datagrams
                        .pop_front()
                        .expect("Expected a pending datagram");
                    pending_group.drop_expired_segments(datagram.kypacket_seq);
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
struct PendingGroup {
    group_seq: u64,
    config_packet: ConfigPacket,
    datagrams: VecDeque<DatagramPacket>,
    segments: Vec<DatagramSegments>,
}

impl PendingGroup {
    fn new(group_seq: u64) -> Self {
        Self {
            group_seq,
            config_packet: ConfigPacket::None,
            datagrams: VecDeque::new(),
            segments: Vec::new(),
        }
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
