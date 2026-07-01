// Project Kyber: lib.rs
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

use std::io;
use std::net::SocketAddr;
use std::str::FromStr;
use url::Url;

#[allow(unused)]
use log::{debug, error, info, warn};

pub mod connection;
mod endpoint;
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

    pub fn parse(url: &str) -> io::Result<Self> {
        #[cold]
        fn invalid_data<E>(err: E) -> io::Error
        where
            E: Into<Box<dyn std::error::Error + Send + Sync>>,
        {
            io::Error::new(io::ErrorKind::InvalidData, err)
        }

        let url = Url::parse(url).map_err(invalid_data)?;

        if url.scheme() != "kymux" {
            return Err(invalid_data("Invalid scheme"));
        }

        let host = url.host().ok_or_else(|| invalid_data("Missing host"))?;
        let ip = match host {
            // Custom scheme don't properly parse IPv4 so they appear as a domain:
            // https://github.com/servo/rust-url/issues/1081.
            url::Host::Domain(domain) => domain.parse().map_err(invalid_data)?,
            url::Host::Ipv4(ip) => ip.into(),
            url::Host::Ipv6(ip) => ip.into(),
        };
        let port = url.port().ok_or_else(|| invalid_data("Missing port"))?;
        let addr = SocketAddr::new(ip, port);

        let mut path_segments = url
            .path_segments()
            .ok_or_else(|| invalid_data("Missing endpoint id"))?;

        let (Some(endpoint), None) = (path_segments.next(), path_segments.next()) else {
            return Err(invalid_data("Expected a single endpoint id"));
        };

        let endpoint_id = u16::from_str_radix(endpoint, 16)
            .map_err(|e| invalid_data(format!("Invalid endpoint ID (hex): {e}")))?;

        Ok(Self { addr, endpoint_id })
    }
}

impl FromStr for KyComAddr {
    type Err = io::Error;

    fn from_str(s: &str) -> io::Result<Self> {
        Self::parse(s)
    }
}

#[cfg(test)]
mod tests {
    use super::KyComAddr;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

    #[test]
    fn parse_ipv4() {
        let port: u16 = 4343;
        let endpoint_id: u16 = 0x0123;

        let uri = format!("kymux://127.0.0.1:{port}/{endpoint_id:X}");
        let addr = KyComAddr::parse(&uri).unwrap();

        assert_eq!(addr.addr.ip(), IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)));
        assert_eq!(addr.addr.port(), port);
        assert_eq!(addr.endpoint_id, endpoint_id);
    }

    #[test]
    fn parse_ipv6() {
        let port: u16 = 4343;
        let endpoint_id: u16 = 0x0123;

        let uri = format!("kymux://[::1]:{port}/{endpoint_id:X}");
        let addr = KyComAddr::parse(&uri).unwrap();

        assert_eq!(
            addr.addr.ip(),
            IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1))
        );
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
