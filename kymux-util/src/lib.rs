// Project Kyber: lib.rs
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

// Utils to be able to use Rc/RefCell on wasm and Arc/Mutex on other platforms.

use std::ops::{Deref, DerefMut};
use thiserror::Error;

#[cfg(not(target_family = "wasm"))]
pub type KyArc<T> = std::sync::Arc<T>;
#[cfg(target_family = "wasm")]
pub type KyArc<T> = std::rc::Rc<T>;

#[cfg(not(target_family = "wasm"))]
#[derive(Debug)]
pub struct KyMutex<T>(std::sync::Mutex<T>);
#[cfg(target_family = "wasm")]
#[derive(Debug)]
pub struct KyMutex<T>(std::cell::RefCell<T>);

#[cfg(not(target_family = "wasm"))]
#[derive(Debug)]
pub struct KyMutexGuard<'a, T: ?Sized + 'a>(std::sync::MutexGuard<'a, T>);
#[cfg(target_family = "wasm")]
#[derive(Debug)]
pub struct KyMutexGuard<'a, T: ?Sized + 'a>(std::cell::RefMut<'a, T>);

impl<T> KyMutex<T> {
    #[inline(always)]
    pub fn new(t: T) -> Self {
        #[cfg(not(target_family = "wasm"))]
        let inner = std::sync::Mutex::new(t);
        #[cfg(target_family = "wasm")]
        let inner = std::cell::RefCell::new(t);

        KyMutex(inner)
    }

    #[inline(always)]
    pub fn lock(&self) -> KyMutexGuard<'_, T> {
        #[cfg(not(target_family = "wasm"))]
        let inner = self.0.lock().unwrap();
        #[cfg(target_family = "wasm")]
        let inner = self.0.borrow_mut();

        KyMutexGuard(inner)
    }
}

impl<T: ?Sized> Deref for KyMutexGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        self.0.deref()
    }
}

impl<T: ?Sized> DerefMut for KyMutexGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        self.0.deref_mut()
    }
}

// KySend is Send on non-wasm platforms
// KySync is Sync on non-wasm platforms

#[cfg(not(target_family = "wasm"))]
pub trait KySend: Send {}

#[cfg(not(target_family = "wasm"))]
pub trait KySync: Sync {}

#[cfg(not(target_family = "wasm"))]
impl<T: Send> KySend for T {}

#[cfg(not(target_family = "wasm"))]
impl<T: Sync> KySync for T {}

#[cfg(target_family = "wasm")]
pub trait KySend {}

#[cfg(target_family = "wasm")]
pub trait KySync {}

#[cfg(target_family = "wasm")]
impl<T> KySend for T {}

#[cfg(target_family = "wasm")]
impl<T> KySync for T {}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
#[error("decode hex error")]
pub enum DecodeHexError {
    OddLength,
    ParseInt(#[from] std::num::ParseIntError),
}

pub fn hex_to_bytes(s: &str) -> Result<Vec<u8>, DecodeHexError> {
    if s.len().is_multiple_of(2) {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).map_err(|e| e.into()))
            .collect()
    } else {
        Err(DecodeHexError::OddLength)
    }
}
