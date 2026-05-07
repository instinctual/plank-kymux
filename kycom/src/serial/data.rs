// Project Kyber: data.rs
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
