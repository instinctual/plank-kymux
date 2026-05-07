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

use std::io::{Error, ErrorKind, Result};
use std::net::SocketAddr;
use std::str::FromStr;
use url::Url;

#[allow(unused)]
use log::{debug, error, info, warn};

pub mod connection;
mod endpoint;
pub mod ipc;
pub mod serial;
mod server;

pub use endpoint::Channel;
pub use kymux_types::*;
pub use server::{ChannelForwarder, Forwarder, KyCom};

struct Task(tokio::task::JoinHandle<()>);

impl Task {
    fn spawn(f: impl std::future::Future<Output = ()> + Send + 'static) -> Self {
        Self(tokio::spawn(f))
    }
}

impl Drop for Task {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct KyComAddr {
    pub addr: SocketAddr,
    pub endpoint_id: u16,
}

impl KyComAddr {
    pub fn new(addr: SocketAddr, endpoint_id: u16) -> Self {
        Self { addr, endpoint_id }
    }

    pub fn url(&self) -> String {
        format!("kymux://{}/{:X}", self.addr, self.endpoint_id)
    }

    pub fn parse(url: &str) -> Result<Self> {
        let url = Url::parse(url).map_err(|_| Error::from(ErrorKind::InvalidData))?;

        if url.scheme() != "kymux" {
            return Err(Error::new(ErrorKind::InvalidData, "Invalid scheme"));
        }

        let host = url
            .host_str()
            .ok_or_else(|| Error::new(ErrorKind::InvalidData, "Missing host"))?;
        let port = url
            .port()
            .ok_or_else(|| Error::new(ErrorKind::InvalidData, "Missing port"))?;

        let addr = format!("{host}:{port}")
            .parse::<SocketAddr>()
            .map_err(|e| {
                Error::new(
                    ErrorKind::InvalidData,
                    format!("Invalid socket address: {e}"),
                )
            })?;

        let path_segments = url
            .path_segments()
            .ok_or_else(|| Error::new(ErrorKind::InvalidData, "Missing endpoint id"))?
            .collect::<Vec<&str>>();

        if path_segments.len() != 1 {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "Expected a single endpoint id",
            ));
        }

        let endpoint_id = u16::from_str_radix(path_segments[0], 16).map_err(|e| {
            Error::new(
                ErrorKind::InvalidData,
                format!("Invalid endpoint ID (hex): {e}"),
            )
        })?;

        Ok(Self { addr, endpoint_id })
    }
}

impl FromStr for KyComAddr {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        Self::parse(s)
    }
}

#[cfg(test)]
mod tests {
    use super::KyComAddr;
    use std::net::{IpAddr, Ipv4Addr};

    #[test]
    fn parse_uri() {
        let port: u16 = 4343;
        let endpoint_id: u16 = 0x0123;

        let uri = format!("kymux://127.0.0.1:{port}/{endpoint_id:X}");
        let addr = KyComAddr::parse(&uri).unwrap();

        assert_eq!(addr.addr.ip(), IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)));
        assert_eq!(addr.addr.port(), port);
        assert_eq!(addr.endpoint_id, endpoint_id);
    }

    #[test]
    #[should_panic]
    fn invalid_scheme() {
        KyComAddr::parse("tcp://127.0.0.1").unwrap();
    }

    #[test]
    #[should_panic]
    fn invalid_host() {
        KyComAddr::parse("kymux://127.0.1:4343/abcd").unwrap();
    }

    #[test]
    #[should_panic]
    fn invalid_port_1() {
        KyComAddr::parse("kymux://127.0.0.1/abcd").unwrap();
    }

    #[test]
    #[should_panic]
    fn invalid_port_2() {
        KyComAddr::parse("kymux://127.0.0.1:aa/abcd").unwrap();
    }

    #[test]
    #[should_panic]
    fn invalid_endpoint_1() {
        KyComAddr::parse("kymux://127.0.0.1:4343").unwrap();
    }

    #[test]
    #[should_panic]
    fn invalid_endpoint_2() {
        KyComAddr::parse("kymux://127.0.0.1:4343//").unwrap();
    }

    #[test]
    #[should_panic]
    fn invalid_endpoint_3() {
        KyComAddr::parse("kymux://127.0.0.1:4343//1234").unwrap();
    }

    #[test]
    #[should_panic]
    fn invalid_endpoint_4() {
        KyComAddr::parse("kymux://127.0.0.1:4343/io").unwrap();
    }
}
