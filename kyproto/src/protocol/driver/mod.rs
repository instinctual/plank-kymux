// Project Kyber: mod.rs
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

pub(crate) mod av;
pub(crate) mod data;
pub(crate) mod input;
pub(crate) mod metrics;
pub(crate) mod util;

use crate::protocol::ProtocolError;

pub(crate) use kymux_types::{ProtocolRecvDriver, ProtocolSendDriver, Serializable};
use kymux_util::{KyAsyncRead, KyAsyncWrite};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};

pub(crate) async fn read_packet<R, T>(reader: &mut R) -> Result<Option<T>, ProtocolError>
where
    R: KyAsyncRead + Unpin,
    T: Serializable,
{
    T::read(reader).await.map_err(ProtocolError::new)
}

pub(crate) async fn write_packet<W, T>(writer: &mut W, packet: T) -> Result<(), ProtocolError>
where
    W: KyAsyncWrite + Unpin,
    T: Serializable,
{
    T::write(packet, writer).await.map_err(ProtocolError::new)?;
    writer.flush().await.map_err(ProtocolError::new)
}
