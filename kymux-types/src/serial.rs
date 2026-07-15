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

use kymux_util::{KyAsyncRead, KyAsyncWrite, KySend};
use std::task::{Poll, Waker};
use std::{future::Future, io};

pub trait Serializable: Send + Sized + 'static {
    fn write(
        &self,
        writer: &mut (dyn KyAsyncWrite + Unpin),
    ) -> impl Future<Output = io::Result<()>> + KySend;

    fn read(
        reader: &mut (dyn KyAsyncRead + Unpin),
    ) -> impl Future<Output = io::Result<Option<Self>>> + KySend;
}

// The format may be different when sending the packet over a transport that
// preserve message boundaries. For example, a _size_ field may be omitted.
pub trait FramedSerializable: Send + Sized + 'static {
    fn serialize(&self) -> Vec<u8>;

    fn parse(msg: &[u8]) -> io::Result<Self>;
}

// Helpers to reuse the Serializable implementation for FramedSerializable

pub trait AutoFramedSerializable: Serializable {}

impl<T> FramedSerializable for T
where
    T: AutoFramedSerializable,
{
    fn serialize(&self) -> Vec<u8> {
        let mut vec = Vec::new();
        poll_expect_ready(self.write(&mut vec)).expect("In-memory serialization cannot fail");
        vec
    }

    fn parse(mut msg: &[u8]) -> io::Result<Self> {
        poll_expect_ready(Self::read(&mut msg))
            .transpose()
            .expect("In-memory deserialization cannot return None")
    }
}

fn poll_expect_ready<F, R>(fut: F) -> R
where
    F: std::future::Future<Output = R>,
{
    let waker = Waker::noop();
    let mut cx = std::task::Context::from_waker(waker);

    let Poll::Ready(res) = std::pin::pin!(fut).poll(&mut cx) else {
        panic!("In-memory Cursor is synchronous");
    };
    res
}
