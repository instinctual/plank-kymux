// Project Kyber: lib.rs
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

use std::io;
use std::net::{IpAddr, SocketAddr};
use std::result::Result;

use log::{debug, warn};
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{tcp, TcpStream};

#[derive(Debug, Error)]
pub enum Error {
    #[error("Invalid URI")]
    InvalidUri,
    #[error("IO error: {source:?}")]
    IOError {
        #[from]
        source: io::Error,
    },
}

fn parse_uri(uri: &str) -> Result<(SocketAddr, u16), Error> {
    let parsed_uri = url::Url::parse(uri).map_err(|e| {
        warn!("Kymux URL parse error: {e:?}");
        Error::InvalidUri
    })?;

    // Check scheme
    if parsed_uri.scheme() != "kymux" {
        warn!("Invalid scheme {}", parsed_uri.scheme());
        return Err(Error::InvalidUri);
    }

    // Parse host/addr
    let Some(host_str) = parsed_uri.host_str() else {
        warn!("URI {uri} has no host");
        return Err(Error::InvalidUri);
    };

    let ip: IpAddr = match host_str.parse() {
        Ok(ip) => ip,
        Err(err) => {
            warn!("URI {uri} IP parsing failed: {err:?}");
            return Err(Error::InvalidUri);
        }
    };

    // Get port
    let Some(port) = parsed_uri.port() else {
        warn!("URI {uri} has no port");
        return Err(Error::InvalidUri);
    };

    // Parse endpoint id
    if parsed_uri.path().is_empty() {
        warn!("URI {uri} has no valid path");
        return Err(Error::InvalidUri);
    }

    let path = &parsed_uri.path()[1..];

    let endpoint_id = match u16::from_str_radix(path, 16) {
        Ok(endpoint_id) => endpoint_id,
        Err(err) => {
            warn!("URI {uri} endrpoint failed: {err:?}");
            return Err(Error::InvalidUri);
        }
    };

    Ok((SocketAddr::new(ip, port), endpoint_id))
}

pub struct Client {
    client: TcpStream,
}

impl Client {
    pub async fn connect(uri: &str) -> Result<Self, Error> {
        let (addr, endpoint_id) = parse_uri(uri)?;
        let mut client = TcpStream::connect(addr).await?;

        // Send handshake
        let b = endpoint_id.to_be_bytes();
        client.write_all(&b).await?;

        // Wait for peer connection
        debug!("Waiting for peer connection");
        let mut b = [0u8];
        client.read_exact(&mut b).await?;

        Ok(Self { client })
    }

    pub fn into_tcp_stream(self) -> TcpStream {
        self.client
    }

    pub fn split(self) -> (tcp::OwnedReadHalf, tcp::OwnedWriteHalf) {
        self.client.into_split()
    }
}

#[cfg(test)]
#[ctor::ctor]
fn init() {
    env_logger::Builder::new()
        .filter_level(log::LevelFilter::Info)
        .format_timestamp_millis()
        .init();
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};

    use super::Error;

    #[test]
    fn parse_uri() {
        let port: u16 = 4343;
        let endpoint_id: u16 = 0x0123;

        let uri = format!("kymux://127.0.0.1:{port}/{endpoint_id:X}");
        let (addr, parsed_endpoint_id) = super::parse_uri(&uri).unwrap();

        assert_eq!(addr.ip(), IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)));
        assert_eq!(addr.port(), port);
        assert_eq!(parsed_endpoint_id, endpoint_id);
    }

    #[test]
    fn invalid_scheme() {
        let ret = super::parse_uri("tcp://127.0.0.1");
        match ret {
            Err(Error::InvalidUri) => {}
            _ => panic!(),
        }
    }

    #[test]
    fn invalid_host() {
        let ret = super::parse_uri("kymux://127.0.1:4343/abcd");
        match ret {
            Err(Error::InvalidUri) => {}
            _ => panic!(),
        }
    }

    #[test]
    fn invalid_port() {
        let ret = super::parse_uri("kymux://127.0.0.1/abcd");
        match ret {
            Err(Error::InvalidUri) => {}
            _ => panic!(),
        }

        let ret = super::parse_uri("kymux://127.0.0.1:aa/abcd");
        match ret {
            Err(Error::InvalidUri) => {}
            _ => panic!(),
        }
    }

    #[test]
    fn invalid_endpoint() {
        let ret = super::parse_uri("kymux://127.0.0.1:4343");
        match ret {
            Err(Error::InvalidUri) => {}
            _ => panic!(),
        }

        let ret = super::parse_uri("kymux://127.0.0.1:4343///");
        match ret {
            Err(Error::InvalidUri) => {}
            _ => panic!(),
        }

        let ret = super::parse_uri("kymux://127.0.0.1:4343//1234");
        match ret {
            Err(Error::InvalidUri) => {}
            _ => panic!(),
        }

        let ret = super::parse_uri("kymux://127.0.0.1:4343/io");
        match ret {
            Err(Error::InvalidUri) => {}
            _ => panic!(),
        }
    }
}
