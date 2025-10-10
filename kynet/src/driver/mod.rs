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

use crate::error::*;
use crate::{Connection, ConnectionStats, KySend, KySync, RecvStream, SendStream};

use std::fmt::Debug;

use async_trait::async_trait;
use bytes::Bytes;

#[cfg(all(feature = "kynet-quinn", not(target_family = "wasm")))]
pub(crate) mod quinn;
#[cfg(all(feature = "kynet-quinn", target_family = "wasm"))]
compile_error!("Quinn is not available for wasm");

#[cfg(all(feature = "kynet-webtransport-js", target_family = "wasm"))]
pub(crate) mod webtransport_js;
#[cfg(all(feature = "kynet-webtransport-js", not(target_family = "wasm")))]
compile_error!("WebTransportJS is only available for wasm");

#[cfg(all(feature = "kynet-wtransport", not(target_family = "wasm")))]
pub(crate) mod wtransport;
#[cfg(all(feature = "kynet-wtransport", target_family = "wasm"))]
compile_error!("WTransport is not available for wasm");

// CommonServer only requires quinn, but can optionally support WebTransport
#[cfg(all(feature = "kynet-quinn", not(target_family = "wasm")))]
pub(crate) mod common;

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
pub trait ConnectionDriver: Debug + KySend + KySync {
    async fn open_uni(&self) -> Result<SendStream, ConnectionError>;

    async fn open_bi(&self) -> Result<(SendStream, RecvStream), ConnectionError>;

    async fn accept_uni(&self) -> Result<RecvStream, ConnectionError>;

    async fn accept_bi(&self) -> Result<(SendStream, RecvStream), ConnectionError>;

    async fn read_datagram(&self) -> Result<Bytes, ConnectionError>;

    async fn send_datagram(&self, data: Bytes) -> Result<(), SendDatagramError>;

    async fn closed(&self) -> Result<(), ConnectionError>;

    fn close(&self, error_code: u32, reason: &str);

    fn max_datagram_size(&self) -> Option<usize>;

    async fn stats(&self) -> ConnectionStats;
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
pub trait SendStreamDriver: Debug + KySend + KySync {
    async fn write(&mut self, buf: &[u8]) -> Result<usize, WriteError>;

    async fn write_all(&mut self, buf: &[u8]) -> Result<(), WriteError> {
        let mut offset = 0;
        while offset < buf.len() {
            let w = self.write(&buf[offset..]).await?;
            assert!(w <= buf.len() - offset);
            offset += w;
            if offset == buf.len() {
                break;
            }
        }
        Ok(())
    }

    async fn finish(&mut self) -> Result<(), ClosedStreamError>;

    fn reset(&mut self);

    async fn closed(&mut self) -> Result<(), ConnectionError>;
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
pub trait RecvStreamDriver: Debug + KySend + KySync {
    async fn read(&mut self, buf: &mut [u8]) -> Result<Option<usize>, ReadError>;

    async fn read_exact(&mut self, buf: &mut [u8]) -> Result<(), ReadExactError> {
        let mut offset = 0;
        while let Some(r) = self.read(&mut buf[offset..]).await? {
            assert!(r <= buf.len() - offset);
            offset += r;
            if offset == buf.len() {
                break;
            }
        }

        assert!(offset <= buf.len());
        if offset < buf.len() {
            Err(ReadExactError::FinishedEarly(offset))?;
        }

        Ok(())
    }

    fn stop(&mut self);

    async fn closed(&mut self) -> Result<(), ConnectionError>;
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
pub trait Server: KySend + KySync {
    async fn accept(&self) -> Result<Connection, ConnectionError>;

    fn close(&self, error_code: u32, reason: &str);

    async fn wait_idle(&self);
}
