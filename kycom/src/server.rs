use crate::ipc;
use crate::serial;
use crate::KyComAddr;

use async_trait::async_trait;
use kyproto_types::ProtocolError;
#[allow(unused)]
use log::{debug, error, info, warn};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::io::{Error, ErrorKind};
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

type EndpointMap = HashMap<u16, oneshot::Sender<TcpStream>>;

pub struct KyCom {
    addr: SocketAddr,
    pending_endpoints: Arc<Mutex<EndpointMap>>,
    listen_task: JoinHandle<()>,
    runner: Option<Runner>,
}

impl KyCom {
    pub async fn start_on_addr(addr: SocketAddr) -> std::io::Result<Self> {
        let pending_endpoints = Arc::new(Mutex::new(HashMap::new()));

        let listener = TcpListener::bind(addr).await?;
        let pending_endpoints2 = pending_endpoints.clone();
        let listen_task = tokio::spawn(async move {
            if let Err(err) = Self::listen(listener, pending_endpoints2).await {
                error!("TcpListener error: {err}");
            }
        });

        Ok(Self {
            addr,
            pending_endpoints,
            listen_task,
            runner: None,
        })
    }

    pub async fn start_on_port(port: u16) -> std::io::Result<Self> {
        let addr = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), port);
        Self::start_on_addr(addr).await
    }

    pub fn register<T>(&self, endpoint: T) -> std::io::Result<TcpForwarder<T>>
    where
        T: kyproto::ProtocolEndpoint,
    {
        let rx = {
            let endpoint_id = endpoint.id();
            let mut pending_endpoints = self.pending_endpoints.lock().unwrap();
            match pending_endpoints.entry(endpoint_id) {
                Entry::Occupied(_) => {
                    return Err(Error::new(
                        ErrorKind::AlreadyExists,
                        "Endpoint {endpoint_id} already pending",
                    ));
                }
                Entry::Vacant(entry) => {
                    let (tx, rx) = oneshot::channel();
                    entry.insert(tx);
                    rx
                }
            }
        };

        Ok(TcpForwarder {
            addr: self.addr,
            rx,
            endpoint,
        })
    }

    pub fn forward_async<Endpoint>(&mut self, forwarder: TcpForwarder<Endpoint>) -> KyComAddr
    where
        Endpoint: kyproto::ProtocolEndpoint + Send + 'static,
        TcpForwarder<Endpoint>: Forwarder,
    {
        let addr = forwarder.addr();

        if self.runner.is_none() {
            // Create on first use
            self.runner = Some(Runner::new());
        }

        let runner = self.runner.as_mut().unwrap();
        runner.forward_async(forwarder);
        addr
    }

    pub fn register_and_forward<Endpoint>(
        &mut self,
        endpoint: Endpoint,
    ) -> std::io::Result<KyComAddr>
    where
        Endpoint: kyproto::ProtocolEndpoint + Send + 'static,
        TcpForwarder<Endpoint>: Forwarder,
    {
        let forwarder = self.register(endpoint)?;
        let addr = self.forward_async(forwarder);
        Ok(addr)
    }

    async fn listen(
        listener: TcpListener,
        pending_endpoints: Arc<Mutex<EndpointMap>>,
    ) -> std::io::Result<()> {
        loop {
            let (tcp_stream, _) = listener.accept().await?;
            let pending_endpoints = pending_endpoints.clone();
            tokio::spawn(async move {
                if let Err(err) = Self::handle_stream(tcp_stream, pending_endpoints).await {
                    error!("TcpStream error: {err}");
                }
            });
        }
    }

    async fn handle_stream(
        mut tcp_stream: TcpStream,
        pending_endpoints: Arc<Mutex<EndpointMap>>,
    ) -> std::io::Result<()> {
        let endpoint_id = tcp_stream.read_u16().await?;
        info!("TCP connection for endpoint {endpoint_id:X}");
        let mut pending_endpoints = pending_endpoints.lock().unwrap();
        if let Some(tx) = pending_endpoints.remove(&endpoint_id) {
            // Ignore error (if the receiver is dropped)
            let _ = tx.send(tcp_stream);
        } else {
            return Err(Error::new(
                ErrorKind::NotFound,
                format!("Connection received for unknown endpoint id: {endpoint_id:X}"),
            ));
        }

        Ok(())
    }

    pub fn stop(self) {
        // drop self
    }
}

impl Drop for KyCom {
    fn drop(&mut self) {
        self.listen_task.abort();
    }
}

struct Task {
    join_handle: JoinHandle<()>,
}

impl Drop for Task {
    fn drop(&mut self) {
        self.join_handle.abort();
    }
}

struct Runner {
    tasks: Vec<Task>,
}

impl Runner {
    fn new() -> Self {
        Self { tasks: Vec::new() }
    }

    fn forward_async<T>(&mut self, forwarder: T)
    where
        T: Forwarder + Send + 'static,
    {
        // Clean up finished tasks
        self.tasks.retain(|task| !task.join_handle.is_finished());

        let forwarder_name = std::any::type_name::<T>()
            .rsplit("::")
            .next()
            .unwrap_or("Unknown");
        debug!("Spawning {forwarder_name} forwarder");

        let join_handle = tokio::spawn(async move {
            if let Err(err) = forwarder.forward().await {
                info!("{forwarder_name} forwarder ended with result {err:?}");
            }
        });

        self.tasks.push(Task { join_handle });
    }
}

pub async fn forward_protocol<T>(
    mut input: kyproto::ProtocolRecv<T>,
    mut output: kyproto::ProtocolSend<T>,
) -> Result<(), ProtocolError> {
    while let Some(packet) = input.recv().await? {
        output.send(packet).await?;
    }

    Ok(())
}

pub async fn forward_protocol_bi<TX: Send + 'static, RX: Send + 'static>(
    proto_send: kyproto::ProtocolSend<RX>,
    proto_recv: kyproto::ProtocolRecv<TX>,
    ipc_send: kyproto::ProtocolSend<TX>,
    ipc_recv: kyproto::ProtocolRecv<RX>,
) -> Result<(), ProtocolError> {
    let send_task = tokio::spawn(async move { forward_protocol(ipc_recv, proto_send).await });
    let recv_task = tokio::spawn(async move { forward_protocol(proto_recv, ipc_send).await });
    let (send_result, recv_result) = tokio::join!(send_task, recv_task);
    let _ = send_result.map_err(ProtocolError::new)?;
    let _ = recv_result.map_err(ProtocolError::new)?;

    Ok(())
}

pub struct TcpForwarder<T: kyproto::ProtocolEndpoint> {
    addr: SocketAddr,
    rx: oneshot::Receiver<TcpStream>,
    endpoint: T,
}

impl<T: kyproto::ProtocolEndpoint> TcpForwarder<T> {
    pub fn addr(&self) -> KyComAddr {
        KyComAddr::new(self.addr, self.endpoint.id())
    }

    async fn start(self) -> Result<(TcpStream, T::Protocol), ProtocolError> {
        let mut tcp_stream = self
            .rx
            .await
            .map_err(|_| ProtocolError::new("TcpStream sender dropped"))?;

        let protocol = self.endpoint.ready().await?;
        tcp_stream
            .write_all(&[0])
            .await
            .map_err(ProtocolError::new)?;

        Ok((tcp_stream, protocol))
    }
}

impl<T> TcpForwarder<T>
where
    T: kyproto::ProtocolEndpoint<Protocol = kyproto::ProtocolRecv<kyproto::AVPacket>>,
{
    async fn forward_client_av_packets(self) -> Result<(), ProtocolError> {
        let (tcp_stream, protocol) = self.start().await?;
        let ipc = ipc::create_send_protocol(tcp_stream, serial::av::AVPacketSerializer);
        forward_protocol(protocol, ipc).await
    }
}

impl<T> TcpForwarder<T>
where
    T: kyproto::ProtocolEndpoint<Protocol = kyproto::ProtocolSend<kyproto::AVPacket>>,
{
    pub async fn forward_server_av_packets(self) -> Result<(), ProtocolError> {
        let (tcp_stream, protocol) = self.start().await?;
        let ipc = ipc::create_recv_protocol(tcp_stream, serial::av::AVPacketDeserializer);
        forward_protocol(ipc, protocol).await
    }
}

#[async_trait]
pub trait Forwarder {
    async fn forward(self) -> Result<(), ProtocolError>;
}

#[async_trait]
impl Forwarder for TcpForwarder<kyproto::VideoClientEndpoint> {
    async fn forward(self) -> Result<(), ProtocolError> {
        self.forward_client_av_packets().await
    }
}

#[async_trait]
impl Forwarder for TcpForwarder<kyproto::VideoServerEndpoint> {
    async fn forward(self) -> Result<(), ProtocolError> {
        self.forward_server_av_packets().await
    }
}

#[async_trait]
impl Forwarder for TcpForwarder<kyproto::AudioClientEndpoint> {
    async fn forward(self) -> Result<(), ProtocolError> {
        self.forward_client_av_packets().await
    }
}

#[async_trait]
impl Forwarder for TcpForwarder<kyproto::AudioServerEndpoint> {
    async fn forward(self) -> Result<(), ProtocolError> {
        self.forward_server_av_packets().await
    }
}

#[async_trait]
impl Forwarder for TcpForwarder<kyproto::DataEndpoint> {
    async fn forward(self) -> Result<(), ProtocolError> {
        let (tcp_stream, (protocol_send, protocol_recv)) = self.start().await?;
        let (ipc_send, ipc_recv) = ipc::create_bi_protocol(
            tcp_stream,
            serial::data::DataPacketSerializer,
            serial::data::DataPacketDeserializer,
        );
        forward_protocol_bi(protocol_send, protocol_recv, ipc_send, ipc_recv).await
    }
}

#[async_trait]
impl Forwarder for TcpForwarder<kyproto::InputEndpoint> {
    async fn forward(self) -> Result<(), ProtocolError> {
        let (tcp_stream, (protocol_send, protocol_recv)) = self.start().await?;
        let (ipc_send, ipc_recv) = ipc::create_bi_protocol(
            tcp_stream,
            serial::input::InputPacketSerializer,
            serial::input::InputPacketDeserializer,
        );
        forward_protocol_bi(protocol_send, protocol_recv, ipc_send, ipc_recv).await
    }
}

#[async_trait]
impl Forwarder for TcpForwarder<kyproto::MetricsServerEndpoint> {
    async fn forward(self) -> Result<(), ProtocolError> {
        let (tcp_stream, protocol) = self.start().await?;
        let ipc = ipc::create_recv_protocol(tcp_stream, serial::metrics::MetricsPacketDeserializer);
        forward_protocol(ipc, protocol).await
    }
}
