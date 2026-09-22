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

use kymux_util::{KyAsyncRead, KyAsyncWrite};

use bytes::{Bytes, BytesMut};
use std::io::{Error, ErrorKind, Result};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Maximum reliable-data payload, excluding the four-byte length prefix.
/// Enforce this at the wire boundary, before allocating a peer-declared size.
pub const MAX_DATA_PACKET_SIZE: usize = 1024 * 1024;

#[derive(Debug, Clone)]
pub struct DataPacket {
    pub payload: Bytes,
}

impl super::Serializable for DataPacket {
    async fn write(self, writer: &mut (dyn KyAsyncWrite + Unpin)) -> Result<()> {
        if !(1..=MAX_DATA_PACKET_SIZE).contains(&self.payload.len()) {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "invalid data packet size",
            ));
        }
        let size = self.payload.len() as u32;

        writer.write_all(&size.to_be_bytes()).await?;
        writer.write_all(&self.payload).await?;

        Ok(())
    }

    async fn read(reader: &mut (dyn KyAsyncRead + Unpin)) -> Result<Option<Self>> {
        let mut buf = [0; 4];
        // Only EOF between records is clean. A partial header is malformed,
        // just like a truncated payload; do not silently accept it as shutdown.
        let read = reader.read(&mut buf).await?;
        if read == 0 {
            return Ok(None);
        }
        reader.read_exact(&mut buf[read..]).await?;
        let size = u32::from_be_bytes(buf) as usize;
        if !(1..=MAX_DATA_PACKET_SIZE).contains(&size) {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "invalid data packet size",
            ));
        }

        let mut buf = BytesMut::zeroed(size);
        reader.read_exact(&mut buf).await?;
        let payload = buf.freeze();

        let packet = DataPacket { payload };

        Ok(Some(packet))
    }
}
