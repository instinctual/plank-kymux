// Project Kyber: clock.rs
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

use thiserror::Error;

#[derive(Debug, Error, Clone)]
#[error("Clock error: {0}")]
pub struct ClockError(pub String);

pub use platform::*;

#[cfg(target_os = "windows")]
mod platform {
    use super::*;
    use windows::Win32::System::Performance::{QueryPerformanceCounter, QueryPerformanceFrequency};

    pub fn now_micros() -> Result<i64, ClockError> {
        unsafe {
            let mut frequency = 0i64;
            QueryPerformanceFrequency(&mut frequency)
                .map_err(|_| ClockError("Query performance frequency error".to_string()))?;

            let mut counter = 0i64;
            QueryPerformanceCounter(&mut counter)
                .map_err(|_| ClockError("Query performance counter error".to_string()))?;

            Ok(counter * 1_000_000 / frequency)
        }
    }
}

#[cfg(target_family = "wasm")]
mod platform {
    use super::*;
    use crate::runtime::Instant;

    pub fn now_micros() -> Result<i64, ClockError> {
        let raw = Instant::now().raw_us();
        raw.try_into()
            .map_err(|_| ClockError("Timestamp does not fit into i64".to_string()))
    }
}

#[cfg(not(any(target_os = "windows", target_family = "wasm")))]
mod platform {
    use super::*;
    use std::mem::MaybeUninit;

    pub fn now_micros() -> Result<i64, ClockError> {
        let timespec = unsafe {
            let mut timespec = MaybeUninit::uninit();
            let r = libc::clock_gettime(libc::CLOCK_MONOTONIC, timespec.as_mut_ptr());
            if r != 0 {
                return Err(ClockError("clock_gettime failed".to_string()));
            }
            timespec.assume_init()
        };
        let result = timespec.tv_sec as i64 * 1_000_000 + timespec.tv_nsec as i64 / 1000;
        Ok(result)
    }
}
