// SPDX-License-Identifier: AGPL-3.0-or-later
//! Bounded kernel receive headroom, shared by native QUIC clients and servers.

use crate::UdpReceiveBuffer;
use socket2::Socket;

pub(crate) const RECEIVE_BUFFER_BYTES: usize = 4 * 1024 * 1024;

pub(super) fn configure_receive_buffer(socket: &Socket) -> std::io::Result<UdpReceiveBuffer> {
    // Linux reports twice the requested SO_RCVBUF value for accounting overhead.
    // Preserve an administrator-provided default that is already large enough.
    let target_reported = RECEIVE_BUFFER_BYTES * if cfg!(target_os = "linux") { 2 } else { 1 };
    if socket.recv_buffer_size()? < target_reported
        && let Err(error) = socket.set_recv_buffer_size(RECEIVE_BUFFER_BYTES)
    {
        // A platform limit must not prevent an otherwise working connection.
        log::warn!("Unable to request QUIC UDP receive buffer: {error}");
    }
    let result = UdpReceiveBuffer {
        requested_bytes: RECEIVE_BUFFER_BYTES,
        reported_bytes: socket.recv_buffer_size()?,
    };
    log::info!("QUIC UDP receive buffer: {result:?}");
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use socket2::{Domain, Protocol, Type};

    #[test]
    fn reports_the_granted_limit_even_when_the_os_caps_the_request() {
        let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP)).unwrap();
        let before = socket.recv_buffer_size().unwrap();
        let result = configure_receive_buffer(&socket).unwrap();
        assert_eq!(result.requested_bytes, 4 * 1024 * 1024);
        assert_eq!(result.reported_bytes, socket.recv_buffer_size().unwrap());
        assert!(result.reported_bytes >= before);
        // A restrictive kernel ceiling is a supported, reported condition.
        // Tests must not assume that SO_RCVBUF grants the requested size.
        eprintln!("quic_udp_receive_buffer={result:?} before={before}");
    }
}
