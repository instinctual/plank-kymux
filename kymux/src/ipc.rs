// Project Kyber: ipc.rs
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

use std::sync::Arc;

use kycom::{Forwarder, TcpForwarder};
use kymux_types::*;
use kyproto::{AudioProtocol, VideoProtocol};

use crate::{Error, Result};

pub struct IpcHandler {
    kycom: kycom::KyCom,
}

impl IpcHandler {
    pub async fn new(local_ports: std::ops::Range<u16>) -> Result<Self> {
        let kycom = kycom::KyCom::start_on_any_port(local_ports)
            .await
            .map_err(|_| Error::IpcNoPortAvailable)?;
        Ok(Self { kycom })
    }

    pub fn stop(self) {
        self.kycom.stop();
    }

    pub fn register_and_forward<T: 'static>(
        &mut self,
        endpoint: ProtocolEndpoint<T>,
    ) -> Result<String>
    where
        TcpForwarder<T>: Forwarder,
    {
        let url = self
            .kycom
            .register_and_forward(endpoint)
            .map(|addr| addr.url())?;
        Ok(url)
    }
}

pub struct IPCForwardableConnection {
    inner: Arc<kyproto::Connection>,
    ipc: IpcHandler,
}

impl IPCForwardableConnection {
    pub async fn new(
        connection: Arc<kyproto::Connection>,
        local_ports: std::ops::Range<u16>,
    ) -> Result<Self> {
        Ok(Self {
            inner: connection,
            ipc: IpcHandler::new(local_ports).await?,
        })
    }

    pub fn stop(self) {
        self.ipc.stop();
    }

    pub async fn closed(&self) -> Result<()> {
        self.inner.closed().await?;
        Ok(())
    }

    pub async fn register_and_forward_video_endpoint(
        &mut self,
        id: Option<u16>,
        video_protocol: VideoProtocol,
    ) -> Result<(u16, String)> {
        let endpoint = self
            .inner
            .register_video_endpoint(id, video_protocol)
            .await?;
        let id = endpoint.id();

        let uri = self.ipc.register_and_forward(endpoint)?;
        Ok((id, uri))
    }

    pub fn connect_and_forward_video_endpoint(
        &mut self,
        id: u16,
        video_protocol: VideoProtocol,
    ) -> Result<String> {
        let endpoint = self.inner.connect_video_endpoint(id, video_protocol)?;

        let uri = self.ipc.register_and_forward(endpoint)?;
        Ok(uri)
    }

    pub async fn register_and_forward_audio_endpoint(
        &mut self,
        id: Option<u16>,
        audio_protocol: AudioProtocol,
    ) -> Result<(u16, String)> {
        let endpoint = self
            .inner
            .register_audio_endpoint(id, audio_protocol)
            .await?;
        let id = endpoint.id();

        let uri = self.ipc.register_and_forward(endpoint)?;
        Ok((id, uri))
    }

    pub fn connect_and_forward_audio_endpoint(
        &mut self,
        id: u16,
        audio_protocol: AudioProtocol,
    ) -> Result<String> {
        let endpoint = self.inner.connect_audio_endpoint(id, audio_protocol)?;

        let uri = self.ipc.register_and_forward(endpoint)?;
        Ok(uri)
    }

    pub async fn register_and_forward_data_endpoint(
        &mut self,
        id: Option<u16>,
    ) -> Result<(u16, String)> {
        let endpoint = self.inner.register_data_endpoint(id).await?;
        let id = endpoint.id();

        let uri = self.ipc.register_and_forward(endpoint)?;
        Ok((id, uri))
    }

    pub fn connect_and_forward_data_endpoint(&mut self, id: u16) -> Result<String> {
        let endpoint = self.inner.connect_data_endpoint(id)?;

        let uri = self.ipc.register_and_forward(endpoint)?;
        Ok(uri)
    }

    pub async fn register_and_forward_input_endpoint(
        &mut self,
        id: Option<u16>,
    ) -> Result<(u16, String)> {
        let endpoint = self.inner.register_input_endpoint(id).await?;
        let id = endpoint.id();

        let uri = self.ipc.register_and_forward(endpoint)?;
        Ok((id, uri))
    }

    pub fn connect_and_forward_input_endpoint(&mut self, id: u16) -> Result<String> {
        let endpoint = self.inner.connect_input_endpoint(id)?;

        let uri = self.ipc.register_and_forward(endpoint)?;
        Ok(uri)
    }

    pub fn connect_metrics_endpoint(&mut self, id: u16) -> Result<MetricsClientEndpoint> {
        Ok(self.inner.connect_metrics_endpoint(id)?)
    }
}
