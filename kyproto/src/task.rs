// Project Kyber: task.rs
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

use crate::runtime;

use std::future::Future;

use kymux_util::*;
#[allow(unused)]
use log::{debug, error, info, warn};
use tokio::sync::oneshot;

pub(crate) struct Task {
    tx: oneshot::Sender<()>,
}

impl Task {
    pub(crate) fn spawn_task<F, S>(task: F, name: S) -> Self
    where
        F: Future<Output = ()> + KySend + 'static,
        S: Into<String>,
    {
        let name = name.into();
        let (tx, rx) = oneshot::channel();

        runtime::spawn(async move {
            tokio::select! {
                _ = rx => {
                    debug!("Task {name} interrupted");
                }
                _ = task => {
                    debug!("Task {name} terminated");
                }
            }
        });

        Task { tx }
    }

    /// Requests cancellation. A failed send just means the task already
    /// stopped on its own; not an error.
    pub(crate) fn cancel(self) {
        let _ = self.tx.send(());
    }
}
