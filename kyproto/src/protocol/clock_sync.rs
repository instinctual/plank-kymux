// Project Kyber: clock_sync.rs
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

use crate::clock;
use crate::router::KyChannel;
use crate::runtime::{self, Duration, SystemTime, UNIX_EPOCH};
use crate::ProtocolError;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use kymux_types as types;

pub type ClockSyncServerEndpoint = types::ProtocolEndpoint<ClockSyncServerProtocol>;
pub type ClockSyncClientEndpoint = types::ProtocolEndpoint<ClockSyncClientProtocol>;

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

fn validate_sync_response(
    response: &Bytes,
    expected_endpoint_id: u16,
) -> Result<(), ProtocolError> {
    if response.len() != 30 {
        return Err(ProtocolError::new(format!(
            "expected 30 byte clock sync response, got {}",
            response.len()
        )));
    }
    let endpoint_id = u16::from_be_bytes([response[0], response[1]]);
    if endpoint_id != expected_endpoint_id {
        return Err(ProtocolError::new(format!(
            "clock sync endpoint id mismatch: expected {}, got {}",
            expected_endpoint_id, endpoint_id
        )));
    }
    Ok(())
}

fn validate_sync_request(datagram: &Bytes) -> Result<(), ProtocolError> {
    if datagram.len() != 14 {
        return Err(ProtocolError::new(format!(
            "expected 14 byte clock sync request, got {}",
            datagram.len()
        )));
    }
    Ok(())
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
                validate_sync_response(&response, self.ky_channel.endpoint_id())?;
                let t4 = clock::now_micros().map_err(ProtocolError::new)?;
                let endpoint_id = response.get_u16();
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
            validate_sync_request(&datagram)?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{BufMut, Bytes, BytesMut};

    #[test]
    fn client_response_wrong_length_returns_error() {
        // A response that is too short (10 bytes instead of 30)
        let response = Bytes::from(vec![0u8; 10]);
        let result = validate_sync_response(&response, 0x0001);
        assert!(result.is_err());
    }

    #[test]
    fn client_response_wrong_endpoint_id_returns_error() {
        // A 30-byte response with endpoint_id=0x0002 but expected 0x0001
        let mut buf = BytesMut::with_capacity(30);
        buf.put_u16(0x0002); // wrong endpoint_id
        buf.put_u32(0); // req_id
        buf.put_i64(0); // t1
        buf.put_i64(0); // t2
        buf.put_i64(0); // t3
        let response = buf.freeze();
        let result = validate_sync_response(&response, 0x0001);
        assert!(result.is_err());
    }

    #[test]
    fn client_response_valid_returns_ok() {
        let mut buf = BytesMut::with_capacity(30);
        buf.put_u16(0x0001); // correct endpoint_id
        buf.put_u32(0); // req_id
        buf.put_i64(100); // t1
        buf.put_i64(200); // t2
        buf.put_i64(300); // t3
        let response = buf.freeze();
        let result = validate_sync_response(&response, 0x0001);
        assert!(result.is_ok());
    }

    #[test]
    fn server_request_wrong_length_returns_error() {
        // A datagram that is too short (5 bytes instead of 14)
        let datagram = Bytes::from(vec![0u8; 5]);
        let result = validate_sync_request(&datagram);
        assert!(result.is_err());
    }

    #[test]
    fn server_request_valid_returns_ok() {
        let datagram = Bytes::from(vec![0u8; 14]);
        let result = validate_sync_request(&datagram);
        assert!(result.is_ok());
    }
}
