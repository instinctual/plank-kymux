// Project Kyber: clock_sync.rs
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

use crate::clock;
use crate::router::KyChannel;
use crate::runtime::{self, Duration, SystemTime, UNIX_EPOCH};
use crate::ProtocolError;

use bytes::{Buf, BufMut, BytesMut};

#[derive(Debug)]
pub struct ClockSyncResult {
    pub t1: i64, // client emission
    pub t2: i64, // server reception
    pub t3: i64, // server emission
    pub t4: i64, // client reception
}

impl ClockSyncResult {
    pub fn new(t1: i64, t2: i64, t3: i64, t4: i64) -> Self {
        Self { t1, t2, t3, t4 }
    }

    pub fn offset_micros(&self) -> i64 {
        (self.t2 - self.t1 + self.t3 - self.t4) / 2
    }

    pub fn delay_micros(&self) -> i64 {
        (self.t4 - self.t1) - (self.t3 - self.t2)
    }
}

pub struct ClockSyncAverage {
    offset_micros: i64,
    delay_micros: i64,
}

impl ClockSyncAverage {
    pub fn offset_micros(&self) -> i64 {
        self.offset_micros
    }

    pub fn delay_micros(&self) -> i64 {
        self.delay_micros
    }
}

pub struct ClockSyncClientProtocol {
    ky_channel: KyChannel,
    next_id: u32,
}

impl ClockSyncClientProtocol {
    pub(crate) fn new(ky_channel: KyChannel) -> Self {
        Self {
            ky_channel,
            next_id: 0,
        }
    }

    pub async fn sync(&mut self) -> Result<Option<ClockSyncResult>, ProtocolError> {
        let req_id = self.next_id;
        self.next_id = self.next_id.wrapping_add(1);

        let mut buf = BytesMut::with_capacity(14);
        buf.put_u16(self.ky_channel.endpoint_id());
        buf.put_u32(req_id);

        let t1 = clock::now_micros().map_err(ProtocolError::new)?;
        buf.put_i64(t1);

        self.ky_channel
            .send_datagram(buf.freeze())
            .await
            .map_err(ProtocolError::new)?;

        let deadline = t1 + 200000; // 200 ms timeout

        loop {
            let now = clock::now_micros().map_err(ProtocolError::new)?;
            if now >= deadline {
                // timeout
                return Ok(None);
            }
            let timeout = Duration::from_micros((deadline - now).try_into().unwrap());
            if let Ok(response) = runtime::timeout(timeout, self.ky_channel.recv_datagram()).await {
                let mut response = response.map_err(ProtocolError::new)?;
                // endpoint_id: 2 bytes
                // req_id: 4 bytes
                // t1: 8 bytes
                // t2: 8 bytes
                // t3: 8 bytes
                assert!(response.len() == 30);
                let t4 = clock::now_micros().map_err(ProtocolError::new)?;
                let endpoint_id = response.get_u16();
                assert!(endpoint_id == self.ky_channel.endpoint_id());
                let id = response.get_u32();
                if req_id == id {
                    let t1 = response.get_i64();
                    let t2 = response.get_i64();
                    let t3 = response.get_i64();
                    return Ok(Some(ClockSyncResult::new(t1, t2, t3, t4)));
                }
            } else {
                // timeout
                return Ok(None);
            }
        }
    }

    pub async fn sync_average(&mut self, times: u32) -> Result<ClockSyncAverage, ProtocolError> {
        assert!(times > 0);
        let mut offset_sum = 0i64;
        let mut delay_sum = 0i64;
        let mut count = 0;
        while count < times {
            if let Some(res) = self.sync().await? {
                offset_sum += res.offset_micros();
                delay_sum += res.delay_micros();
                count += 1;
            }
        }

        let offset_micros = offset_sum / count as i64;
        let delay_micros = delay_sum / count as i64;
        let avg = ClockSyncAverage {
            offset_micros,
            delay_micros,
        };
        Ok(avg)
    }
}

pub struct ClockSyncServerProtocol {
    ky_channel: KyChannel,
}

impl ClockSyncServerProtocol {
    pub(crate) fn new(ky_channel: KyChannel) -> Self {
        Self { ky_channel }
    }

    pub async fn serve(&mut self) -> Result<(), ProtocolError> {
        loop {
            let datagram = self
                .ky_channel
                .recv_datagram()
                .await
                .map_err(ProtocolError::new)?;
            // endpoint_id: 2 bytes
            // req_id: 4 bytes
            // t1: 8 bytes
            assert!(datagram.len() == 14);
            let t2 = clock::now_micros().map_err(ProtocolError::new)?;

            let mut buf = BytesMut::with_capacity(30);
            buf.extend_from_slice(&datagram);
            buf.put_i64(t2);

            let t3 = clock::now_micros().map_err(ProtocolError::new)?;

            buf.put_i64(t3);
            self.ky_channel
                .send_datagram(buf.freeze())
                .await
                .map_err(ProtocolError::new)?;
        }
    }
}
