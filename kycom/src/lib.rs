use std::io::{Error, ErrorKind, Result};
use std::net::SocketAddr;
use std::str::FromStr;
use url::Url;

#[allow(unused)]
use log::{debug, error, info, warn};

#[cfg(feature = "client")]
mod client;
pub mod ipc;
pub mod serial;
#[cfg(feature = "server")]
mod server;

#[cfg(feature = "client")]
pub use client::{
    DataEndpoint, InputEndpoint, MetricsClientEndpoint, MetricsServerEndpoint, VideoClientEndpoint,
    VideoServerEndpoint,
};
pub use kyproto_types::av::*;
pub use kyproto_types::data::*;
pub use kyproto_types::input::*;
pub use kyproto_types::metrics::*;
pub use kyproto_types::*;
#[cfg(feature = "server")]
pub use server::{Forwarder, KyCom, TcpForwarder};

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
