// Project Kyber: error.rs
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

use thiserror::Error;

#[derive(Debug, Error, Clone, Eq, PartialEq)]
#[error("connection error: {0}")]
pub struct ConnectionError(pub String);

#[derive(Debug, Error, Clone, Eq, PartialEq)]
#[error("send datagram error: {0}")]
pub struct SendDatagramError(pub String);

#[derive(Debug, Error, Clone, Eq, PartialEq)]
#[error("read error: {0}")]
pub struct ReadError(pub String);

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum ReadExactError {
    #[error("read exact finished early ({0} bytes read)")]
    FinishedEarly(usize),
    #[error("read error")]
    ReadError(#[from] ReadError),
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
#[error("write error: {0}")]
pub struct WriteError(pub String);

#[derive(Debug, Error, Clone, Eq, PartialEq)]
#[error("closed stream")]
pub struct ClosedStreamError;

impl From<std::io::Error> for ConnectionError {
    fn from(value: std::io::Error) -> Self {
        Self(value.to_string())
    }
}
