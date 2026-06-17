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

use std::future::Future;

#[cfg(target_family = "wasm")]
use futures_util::future;

#[cfg(not(target_family = "wasm"))]
pub use tokio::task::JoinHandle;

#[cfg(target_family = "wasm")]
pub struct JoinHandle<T> {
    abort_handle: future::AbortHandle,
    _marker: std::marker::PhantomData<T>,
}

#[cfg(target_family = "wasm")]
impl<T> JoinHandle<T> {
    pub fn abort(&self) {
        self.abort_handle.abort();
    }
}

#[cfg(not(target_family = "wasm"))]
pub fn spawn<F>(future: F) -> JoinHandle<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    tokio::spawn(future)
}

#[cfg(target_family = "wasm")]
pub fn spawn<F>(future: F) -> JoinHandle<()>
where
    F: Future<Output = ()> + 'static,
{
    let (abort_handle, abort_registration) = future::AbortHandle::new_pair();
    let abortable_future = future::Abortable::new(future, abort_registration);

    wasm_bindgen_futures::spawn_local(async {
        let _ = abortable_future.await;
    });

    JoinHandle {
        abort_handle,
        _marker: std::marker::PhantomData,
    }
}
