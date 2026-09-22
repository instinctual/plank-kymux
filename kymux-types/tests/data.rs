// SPDX-License-Identifier: AGPL-3.0-or-later

use bytes::Bytes;
use kymux_types::{DataPacket, MAX_DATA_PACKET_SIZE, Serializable};
use std::io::{self, ErrorKind};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, ReadBuf};

// A peer sends only a fragmented length prefix, then withholds its payload.
// Oversized lengths must fail immediately, without polling the payload reader.
struct HeaderOnly {
    header: [u8; 4],
    read: usize,
}

impl AsyncRead for HeaderOnly {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        assert!(self.read < 4, "invalid length reached the payload reader");
        buf.put_slice(&self.header[self.read..self.read + 1]);
        self.read += 1;
        Poll::Ready(Ok(()))
    }
}

#[tokio::test]
async fn rejects_invalid_lengths_before_reading_payload() {
    assert_eq!(MAX_DATA_PACKET_SIZE, 1_048_576);
    for size in [
        0,
        MAX_DATA_PACKET_SIZE as u32 + 1,
        i32::MAX as u32,
        u32::MAX,
    ] {
        let mut reader = HeaderOnly {
            header: size.to_be_bytes(),
            read: 0,
        };
        assert_eq!(
            DataPacket::read(&mut reader).await.unwrap_err().kind(),
            ErrorKind::InvalidData
        );
        assert_eq!(reader.read, 4);
    }
}

#[tokio::test]
async fn rejects_outgoing_invalid_sizes_without_writing_header() {
    for size in [0, MAX_DATA_PACKET_SIZE + 1] {
        let mut wire = Vec::new();
        let packet = DataPacket {
            payload: Bytes::from(vec![7; size]),
        };
        assert_eq!(
            packet.write(&mut wire).await.unwrap_err().kind(),
            ErrorKind::InvalidInput
        );
        assert!(wire.is_empty());
    }
}

#[tokio::test]
async fn preserves_boundary_packets_and_record_order() {
    let mut wire = Vec::new();
    for size in [1, 512 * 1024, MAX_DATA_PACKET_SIZE] {
        DataPacket {
            payload: Bytes::from(vec![0x5a; size]),
        }
        .write(&mut wire)
        .await
        .unwrap();
    }
    let mut reader = wire.as_slice();
    for size in [1, 512 * 1024, MAX_DATA_PACKET_SIZE] {
        let packet = DataPacket::read(&mut reader).await.unwrap().unwrap();
        assert_eq!(packet.payload.len(), size);
        assert!(packet.payload.iter().all(|byte| *byte == 0x5a));
    }
    assert!(DataPacket::read(&mut reader).await.unwrap().is_none());
}

#[tokio::test]
async fn keeps_existing_big_endian_wire_format() {
    let mut wire = Vec::new();
    DataPacket {
        payload: Bytes::from_static(b"abc"),
    }
    .write(&mut wire)
    .await
    .unwrap();
    assert_eq!(wire, b"\x00\x00\x00\x03abc");
    let mut reader = wire.as_slice();
    assert_eq!(
        DataPacket::read(&mut reader)
            .await
            .unwrap()
            .unwrap()
            .payload,
        b"abc"[..]
    );
}

#[tokio::test]
async fn eof_is_clean_only_between_records() {
    let mut empty = &b""[..];
    assert!(DataPacket::read(&mut empty).await.unwrap().is_none());
    let prefix = 5u32.to_be_bytes();
    for size in 1..4 {
        let mut reader = &prefix[..size];
        assert_eq!(
            DataPacket::read(&mut reader).await.unwrap_err().kind(),
            ErrorKind::UnexpectedEof
        );
    }
    let mut truncated = &b"\x00\x00\x00\x05abc"[..];
    assert_eq!(
        DataPacket::read(&mut truncated).await.unwrap_err().kind(),
        ErrorKind::UnexpectedEof
    );
}

#[tokio::test]
async fn accepts_fragmented_valid_records() {
    // A one-byte duplex capacity forces the prefix and payload across reads.
    let (mut writer, mut reader) = tokio::io::duplex(1);
    let packet = DataPacket {
        payload: Bytes::from_static(b"control"),
    };
    let (sent, received) = tokio::join!(packet.write(&mut writer), DataPacket::read(&mut reader));
    sent.unwrap();
    assert_eq!(received.unwrap().unwrap().payload, b"control"[..]);
}
