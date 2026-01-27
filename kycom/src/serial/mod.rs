// Project Kyber: mod.rs
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

use async_trait::async_trait;
use std::io::Result;
use tokio::io::{AsyncRead, AsyncWrite};

pub mod av;
pub mod data;
pub mod input;
pub mod metrics;

#[async_trait]
pub trait Serializer {
    type Packet;
    async fn write(
        &mut self,
        packet: Self::Packet,
        writer: &mut (dyn AsyncWrite + Send + Unpin),
    ) -> Result<()>;
}

#[async_trait]
pub trait Deserializer {
    type Packet;
    async fn read(
        &mut self,
        reader: &mut (dyn AsyncRead + Send + Unpin),
    ) -> Result<Option<Self::Packet>>;
}
