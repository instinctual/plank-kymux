// Project Kyber: metrics.rs
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
use kymux_types::metrics::*;
use std::io::{ErrorKind, Result};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

#[allow(dead_code)]
pub struct MetricsPacketSerializer;
pub struct MetricsPacketDeserializer;

#[async_trait]
impl Serializer for MetricsPacketSerializer {
    type Packet = MetricsPacket;

    async fn write(
        &mut self,
        packet: Self::Packet,
        writer: &mut (dyn AsyncWrite + Send + Unpin),
    ) -> Result<()> {
        let size =
            u16::try_from(packet.payload.len()).expect("Metrics packet size must fit in 16 bits");

        writer.write_all(&size.to_be_bytes()).await?;
        writer.write_all(&packet.payload).await?;

        Ok(())
    }
}

#[async_trait]
impl Deserializer for MetricsPacketDeserializer {
    type Packet = MetricsPacket;

    async fn read(
        &mut self,
        reader: &mut (dyn AsyncRead + Send + Unpin),
    ) -> Result<Option<Self::Packet>> {
        let mut buf = [0; 2];
        if let Err(err) = reader.read_exact(&mut buf).await {
            if err.kind() == ErrorKind::UnexpectedEof {
                return Ok(None); // EOF
            }
            return Err(err);
        }
        let size = u16::from_be_bytes(buf);

        let mut buf = BytesMut::zeroed(size as usize);
        reader.read_exact(&mut buf).await?;
        let payload = buf.freeze();

        let packet = MetricsPacket { payload };

        Ok(Some(packet))
    }
}
