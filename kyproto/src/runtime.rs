// Project Kyber: runtime.rs
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

#[cfg(target_family = "wasm")]
pub use kywasmtime::{Instant, SystemTime, UNIX_EPOCH, sleep, sleep_until, timeout, timeout_at};
pub use std::time::Duration;
#[cfg(not(target_family = "wasm"))]
pub use std::time::{SystemTime, UNIX_EPOCH};
#[cfg(not(target_family = "wasm"))]
pub use tokio::time::{Instant, sleep, sleep_until, timeout, timeout_at};

pub use kymux_types::runtime::*;
