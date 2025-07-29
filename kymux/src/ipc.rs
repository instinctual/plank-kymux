use kyproto::{AudioProtocol, MetricsClientEndpoint, ProtocolEndpoint, VideoProtocol};
use std::sync::Arc;

use kycom::{Forwarder, TcpForwarder};

use crate::{Error, Result};

pub struct IpcHandler {
    kycom: kycom::KyCom,
}

impl IpcHandler {
    pub async fn new(local_ports: std::ops::Range<u16>) -> Result<Self> {
        for port in local_ports {
            let kycom = kycom::KyCom::start_on_port(port).await;
            match kycom {
                Ok(kycom) => return Ok(Self { kycom }),
                Err(err) => {
                    log::warn!("Fail to set IpcHandler on port {port}: {err:?}");
                }
            }
        }
        Err(Error::IpcNoPortAvailable)
    }

    pub fn stop(self) {
        self.kycom.stop();
    }

    pub fn register_and_forward<Endpoint>(&mut self, endpoint: Endpoint) -> Result<String>
    where
        Endpoint: ProtocolEndpoint + Send + 'static,
        TcpForwarder<Endpoint>: Forwarder,
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
