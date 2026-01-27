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

#[derive(Debug, Error)]
pub enum Error {
    #[error("Unimplemented function")]
    NotImplemented,
    #[error("Kyproto connection error: {source:?}")]
    KyprotoConnectionError {
        #[from]
        source: kyproto::ConnectionError,
    },
    #[error("Kyproto protocol error: {source:?}")]
    KyprotoProtocolError {
        #[from]
        source: kyproto::ProtocolError,
    },
    #[error("IO Error  {source:?}")]
    IoError {
        #[from]
        source: std::io::Error,
    },
    #[cfg(feature = "backend-quinn")]
    #[error("Rustls error: {source:?}")]
    TlsError {
        #[from]
        source: rustls::Error,
    },
    #[cfg(feature = "backend-webtransport-js")]
    #[error("WebtransportJS failed to decode hexstring: {source:?}")]
    DecodeHexError {
        #[from]
        source: kyproto::DecodeHexError,
    },
    #[error("Endpoint creation has failed: {source:?}")]
    EndpointCreateFailed { source: std::io::Error },
    #[error("Thread has panicked")]
    ThreadPanicked,
    #[error("No port available for local IPC")]
    IpcNoPortAvailable,
}

impl<T> From<std::sync::PoisonError<T>> for Error {
    fn from(_: std::sync::PoisonError<T>) -> Error {
        Error::ThreadPanicked
    }
}

pub type Result<T> = std::result::Result<T, Error>;
