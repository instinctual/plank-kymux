// Project Kyber: av.rs
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

use crate::serial::{Deserializer, Serializer};

use kymux_util::{KyAsyncRead, KyAsyncWrite};

use async_trait::async_trait;
use byteorder::{BigEndian, ByteOrder};
use bytes::{Bytes, BytesMut};
use std::io::{ErrorKind, Result};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Debug, Clone)]
pub enum AVPacket {
    Codec(CodecPacket),
    Media(MediaPacket),
    Hole(HolePacket),
}

#[derive(Debug, Clone)]
pub enum AVPacketHeader {
    Codec(CodecPacketHeader),
    Media(MediaPacketHeader),
    Hole(HolePacketHeader),
}

impl AVPacketHeader {
    pub const SERIALIZED_SIZE: usize = 12;
}

#[derive(Debug, Clone)]
pub struct CodecPacket {
    pub header: CodecPacketHeader,
}

#[derive(Debug, Clone)]
pub struct CodecPacketHeader {
    pub codec: u32,
    pub rotation: u8,
    pub frame_size: u16,
}

#[derive(Debug, Clone)]
pub struct MediaPacket {
    pub header: MediaPacketHeader,
    pub payload: Bytes,
}

#[derive(Debug, Clone)]
pub struct MediaPacketHeader {
    pub is_config: bool,
    pub is_key: bool,
    pub pts: u64,
    pub size: u32,
}

#[derive(Debug, Clone)]
pub struct HolePacket {
    pub header: HolePacketHeader,
}

#[derive(Debug, Clone)]
pub struct HolePacketHeader {
    pub missing_audio_samples: u32,
}

impl CodecPacketHeader {
    pub fn serialize_to(&self, buf: &mut [u8]) {
        assert!(buf.len() == AVPacketHeader::SERIALIZED_SIZE);

        BigEndian::write_u32(&mut buf[..4], self.codec);
        buf[4] = self.rotation;
        BigEndian::write_u16(&mut buf[5..7], self.frame_size);
        buf[7..].fill(0);
    }

    pub fn serialize(&self) -> [u8; AVPacketHeader::SERIALIZED_SIZE] {
        let mut buf = [0; AVPacketHeader::SERIALIZED_SIZE];
        self.serialize_to(&mut buf);
        buf
    }

    pub fn deserialize(buf: &[u8]) -> Self {
        assert!(buf.len() == AVPacketHeader::SERIALIZED_SIZE);
        assert!(buf[0] & 0x80 == 0); // codec packet

        let codec = BigEndian::read_u32(&buf[..4]);
        let rotation = buf[4];
        let frame_size = BigEndian::read_u16(&buf[5..7]);
        Self {
            codec,
            rotation,
            frame_size,
        }
    }
}

impl MediaPacketHeader {
    pub fn serialize_to(&self, buf: &mut [u8]) {
        assert!(buf.len() == AVPacketHeader::SERIALIZED_SIZE);

        assert!(self.pts & !0x1F_FF_FF_FF_FF_FF_FF_FF == 0);
        let mut pts_and_flags = self.pts;
        pts_and_flags |= 1 << 63; // media packet
        if self.is_config {
            pts_and_flags |= 1 << 62;
        }
        if self.is_key {
            pts_and_flags |= 1 << 61;
        }

        BigEndian::write_u64(&mut buf[..8], pts_and_flags);
        BigEndian::write_u32(&mut buf[8..], self.size);
    }

    pub fn serialize(&self) -> [u8; AVPacketHeader::SERIALIZED_SIZE] {
        let mut buf = [0; AVPacketHeader::SERIALIZED_SIZE];
        self.serialize_to(&mut buf);
        buf
    }

    pub fn deserialize(buf: &[u8]) -> Self {
        assert!(buf.len() == AVPacketHeader::SERIALIZED_SIZE);
        assert!(buf[0] & 0x80 != 0); // media packet

        let pts_and_flags = BigEndian::read_u64(&buf[..8]);
        let is_config = (pts_and_flags & (1 << 62)) != 0;
        let is_key = (pts_and_flags & (1 << 61)) != 0;
        let pts = pts_and_flags & ((1 << 61) - 1);
        let size = BigEndian::read_u32(&buf[8..]);

        MediaPacketHeader {
            is_config,
            is_key,
            pts,
            size,
        }
    }
}

impl HolePacketHeader {
    pub fn serialize_to(&self, buf: &mut [u8]) {
        assert!(buf.len() == AVPacketHeader::SERIALIZED_SIZE);

        buf[..4].fill(0);
        BigEndian::write_u32(&mut buf[4..8], self.missing_audio_samples);
        buf[8..].fill(0);
    }

    pub fn serialize(&self) -> [u8; AVPacketHeader::SERIALIZED_SIZE] {
        let mut buf = [0; AVPacketHeader::SERIALIZED_SIZE];
        self.serialize_to(&mut buf);
        buf
    }

    pub fn deserialize(buf: &[u8]) -> Self {
        assert!(buf.len() == AVPacketHeader::SERIALIZED_SIZE);
        assert!(buf[0] == 0 && buf[1] == 0 && buf[2] == 0 && buf[3] == 0); // missing-info packet

        let missing_audio_samples = BigEndian::read_u32(&buf[4..8]);
        // never use such a packet if there are no missing samples
        assert!(missing_audio_samples > 0);

        Self {
            missing_audio_samples,
        }
    }
}

impl AVPacketHeader {
    pub fn deserialize(buf: &[u8]) -> Self {
        assert!(buf.len() == AVPacketHeader::SERIALIZED_SIZE);

        let is_media = buf[0] & 0x80 != 0;
        if is_media {
            Self::Media(MediaPacketHeader::deserialize(buf))
        } else if buf[0] == 0 && buf[1] == 0 && buf[2] == 0 && buf[3] == 0 {
            Self::Hole(HolePacketHeader::deserialize(buf))
        } else {
            Self::Codec(CodecPacketHeader::deserialize(buf))
        }
    }
}

pub struct AVPacketSerializer;
pub struct AVPacketDeserializer;

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl Serializer for AVPacketSerializer {
    type Packet = AVPacket;

    async fn write(
        &mut self,
        packet: Self::Packet,
        writer: &mut (dyn KyAsyncWrite + Unpin),
    ) -> Result<()> {
        match packet {
            AVPacket::Codec(packet) => {
                let header = packet.header.serialize();
                writer.write_all(&header).await?;
            }
            AVPacket::Media(packet) => {
                let header = packet.header.serialize();
                writer.write_all(&header).await?;
                writer.write_all(&packet.payload).await?;
            }
            AVPacket::Hole(packet) => {
                let header = packet.header.serialize();
                writer.write_all(&header).await?;
            }
        }

        Ok(())
    }
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl Deserializer for AVPacketDeserializer {
    type Packet = AVPacket;

    async fn read(
        &mut self,
        reader: &mut (dyn KyAsyncRead + Unpin),
    ) -> Result<Option<Self::Packet>> {
        let mut header = [0; AVPacketHeader::SERIALIZED_SIZE];
        if let Err(err) = reader.read_exact(&mut header).await {
            if err.kind() == ErrorKind::UnexpectedEof {
                return Ok(None); // EOF
            } else {
                return Err(err);
            }
        }

        let header = AVPacketHeader::deserialize(&header);
        let packet = match header {
            AVPacketHeader::Media(header) => {
                let mut buf = BytesMut::zeroed(header.size as usize);

                reader.read_exact(&mut buf).await?;

                AVPacket::Media(MediaPacket {
                    header,
                    payload: buf.freeze(),
                })
            }
            AVPacketHeader::Codec(header) => AVPacket::Codec(CodecPacket { header }),
            AVPacketHeader::Hole(header) => AVPacket::Hole(HolePacket { header }),
        };

        Ok(Some(packet))
    }
}
