// Project Kyber: auth.rs
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

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::INITIATOR_SERVER;
use crate::{Connection, ConnectionError, Control, Router};

// Define a maximum token len. Current value is an arbitrary value set
// to 1kB.
// For example, Apache and nginx have default header length set to 8kB.
pub const TOKEN_MAX_LEN: usize = 1024;

#[derive(Debug, Error, Clone, Eq, PartialEq)]
#[error("invalid token size")]
pub struct TokenSizeInvalid;

#[derive(Serialize, Deserialize, Eq, PartialEq)]
pub struct ClientAuth {
    token: String,
}

impl ClientAuth {
    /// Token longer than TOKEN_MAX_LEN will be rejected
    pub fn new(token: &str) -> Result<Self, TokenSizeInvalid> {
        if token.len() > TOKEN_MAX_LEN {
            return Err(TokenSizeInvalid);
        }

        Ok(Self {
            token: token.to_string(),
        })
    }

    pub fn token(&self) -> &str {
        &self.token
    }
}

pub(crate) fn reject_authentication(conn: &kynet::Connection) {
    // 0xc is APPLICATION_ERROR (RFC 9000 §20.1)
    conn.close(0xc, "Application token mismatch");
}

pub struct UnauthenticatedConnection {
    conn: kynet::Connection,
    token: ClientAuth,
    tx: kynet::SendStream,
    rx: kynet::RecvStream,
}

impl UnauthenticatedConnection {
    pub(crate) fn new(
        conn: kynet::Connection,
        token: ClientAuth,
        tx: kynet::SendStream,
        rx: kynet::RecvStream,
    ) -> Self {
        Self {
            conn,
            token,
            tx,
            rx,
        }
    }

    pub fn get_auth(&self) -> &ClientAuth {
        &self.token
    }

    pub async fn accept_authentication(self) -> Result<Connection, ConnectionError> {
        let Self {
            conn, mut tx, rx, ..
        } = self;

        // Write 1 byte to confirm authentication (the connection is closed on rejection)
        tx.write(&[0])
            .await
            .map_err(|_| ConnectionError("Dummy byte write failed".to_string()))?;
        tx.flush()
            .await
            .map_err(|_| ConnectionError("Dummy byte flush failed".to_string()))?;

        let control = Control::start(tx, rx);
        let router = Router::start(conn.clone());
        Ok(Connection::new(conn, router, control, INITIATOR_SERVER))
    }

    pub fn reject_authentication(self) {
        reject_authentication(&self.conn);
    }
}
