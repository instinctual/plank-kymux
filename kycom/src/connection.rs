// Project Kyber: connection.rs
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

use crate::KyComAddr;
#[allow(unused)]
use log::{debug, error, info, warn};
use std::net::{Ipv4Addr, SocketAddr};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

pub(crate) struct Connection {
    pub addr: KyComAddr,
    pub read: Box<dyn AsyncRead + Send + Unpin + 'static>,
    pub write: Box<dyn AsyncWrite + Send + Unpin + 'static>,
}

impl Connection {
    pub async fn connect(addr: KyComAddr) -> std::io::Result<Self> {
        let tcp_stream = TcpStream::connect(addr.addr).await?;
        let (read, write) = tcp_stream.into_split();
        Ok(Self {
            addr,
            read: Box::new(read),
            write: Box::new(write),
        })
    }
}

impl AsyncWrite for Connection {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        Pin::new(&mut this.write).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        Pin::new(&mut this.write).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        Pin::new(&mut this.write).poll_shutdown(cx)
    }
}

impl AsyncRead for Connection {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        Pin::new(&mut this.read).poll_read(cx, buf)
    }
}

pub(crate) struct Server {
    pub addr: SocketAddr,
    rx: mpsc::Receiver<(TcpStream, u16)>,
    listen_task: JoinHandle<()>,
}

impl Server {
    pub async fn start_on_addr(addr: SocketAddr) -> std::io::Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        let (tx, rx) = mpsc::channel(16);
        let listen_task = tokio::spawn(async move {
            if let Err(err) = Self::listen(listener, tx).await {
                error!("TcpListener error: {err}");
            }
        });

        Ok(Self {
            addr,
            rx,
            listen_task,
        })
    }

    pub async fn start_on_port(port: u16) -> std::io::Result<Self> {
        let addr = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), port);
        Self::start_on_addr(addr).await
    }

    pub async fn start_on_any_port(local_ports: std::ops::Range<u16>) -> std::io::Result<Self> {
        for port in local_ports {
            let server = Self::start_on_port(port).await;
            match server {
                Ok(server) => return Ok(server),
                Err(err) => {
                    warn!("Fail to listen on port {port}: {err:?}");
                }
            }
        }

        Err(std::io::Error::from(std::io::ErrorKind::AddrInUse))
    }

    pub async fn accept(&mut self) -> std::io::Result<Connection> {
        let Some((tcp_stream, endpoint_id)) = self.rx.recv().await else {
            return Err(std::io::Error::from(std::io::ErrorKind::ConnectionAborted));
        };

        let addr = KyComAddr::new(self.addr, endpoint_id);
        let (read, write) = tcp_stream.into_split();
        Ok(Connection {
            addr,
            read: Box::new(read),
            write: Box::new(write),
        })
    }

    async fn listen(
        listener: TcpListener,
        tx: mpsc::Sender<(TcpStream, u16)>,
    ) -> std::io::Result<()> {
        loop {
            let (tcp_stream, _) = listener.accept().await?;
            let tx = tx.clone();
            tokio::spawn(async move {
                if let Err(err) = Self::handle_stream(tcp_stream, tx).await {
                    error!("TcpStream error: {err}");
                }
            });
        }
    }

    async fn handle_stream(
        mut tcp_stream: TcpStream,
        tx: mpsc::Sender<(TcpStream, u16)>,
    ) -> std::io::Result<()> {
        let endpoint_id = tcp_stream.read_u16().await?;
        info!("TCP connection for endpoint {endpoint_id:X}");

        // Ignore error (if the receiver is dropped)
        let _ = tx.send((tcp_stream, endpoint_id)).await;

        Ok(())
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.listen_task.abort();
    }
}
