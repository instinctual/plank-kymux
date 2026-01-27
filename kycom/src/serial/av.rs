// Project Kyber: av.rs
// Copyright © 2022-2026 Kyber SAS
// SPDX-License-Identifier: LicenseRef-Kyber-Commercial OR AGPL-3.0
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

use super::{Deserializer, Serializer};
use async_trait::async_trait;
use bytes::BytesMut;
use kymux_types::av::*;
use std::io::{ErrorKind, Result};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub struct AVPacketSerializer;
pub struct AVPacketDeserializer;

#[async_trait]
impl Serializer for AVPacketSerializer {
    type Packet = AVPacket;

    async fn write(
        &mut self,
        packet: Self::Packet,
        writer: &mut (dyn AsyncWrite + Send + Unpin),
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

#[async_trait]
impl Deserializer for AVPacketDeserializer {
    type Packet = AVPacket;

    async fn read(
        &mut self,
        reader: &mut (dyn AsyncRead + Send + Unpin),
    ) -> Result<Option<Self::Packet>> {
        let mut header = [0; AVPacketHeader::SERIALIZED_SIZE];
        if let Err(err) = reader.read_exact(&mut header).await {
            if err.kind() == ErrorKind::UnexpectedEof {
                return Ok(None); // EOF
            } else {
                return Err(err);
            }
        }

        // XXX The header serialization format is specific to the IPC
        // (KyCom), but for convenience we use the same in KyProto
        // protocols implementation, so the code is shared
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
