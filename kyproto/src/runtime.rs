// Project Kyber: runtime.rs
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

use std::future::Future;

#[cfg(target_family = "wasm")]
pub use kywasmtime::{sleep, sleep_until, timeout, timeout_at, Instant, SystemTime, UNIX_EPOCH};
#[cfg(target_family = "wasm")]
pub use std::time::Duration;
#[cfg(not(target_family = "wasm"))]
pub use std::time::{SystemTime, UNIX_EPOCH};
#[cfg(not(target_family = "wasm"))]
pub use tokio::time::{sleep, sleep_until, timeout, timeout_at, Duration, Instant};

#[cfg(all(feature = "tokio-rt", not(target_family = "wasm")))]
pub fn spawn<F>(future: F)
where
    F: Future<Output = ()> + Send + 'static,
{
    tokio::spawn(future);
}
#[cfg(all(feature = "tokio-rt", target_family = "wasm"))]
compile_error!("Tokio runtime is not available for wasm");

#[cfg(all(feature = "js", target_family = "wasm"))]
pub fn spawn<F>(future: F)
where
    F: Future<Output = ()> + 'static,
{
    wasm_bindgen_futures::spawn_local(future);
}
#[cfg(all(feature = "js", not(target_family = "wasm")))]
compile_error!("JS runtime is only available for wasm");
