// SPDX-License-Identifier: AGPL-3.0-or-later
use super::*;

pub(in super::super) fn media_bytes(size: usize) -> Vec<u8> {
    let header = MediaPacketHeader {
        is_config: false,
        is_key: true,
        pts: 123,
        size: size as u32,
    };
    let mut data = header.serialize().to_vec();
    data.extend((0..size).map(|i| (i % 251) as u8));
    data
}

pub(in super::super) fn datagram(header_size: usize, oti: Oti, packet: EncodingPacket) -> Bytes {
    let mut bytes = vec![0; header_size - 16];
    bytes.extend(oti.serialize());
    bytes.extend(packet.serialize());
    Bytes::from(bytes)
}

fn raw_oti(length: u64, symbol: u16, blocks: u8, sub_blocks: u16, alignment: u8) -> Oti {
    // Deliberately bypass the asserting OTI constructor as a peer can.
    let mut raw = [0; 12];
    raw[..5].copy_from_slice(&length.to_be_bytes()[3..]);
    raw[6..8].copy_from_slice(&symbol.to_be_bytes());
    raw[8] = blocks;
    raw[9..11].copy_from_slice(&sub_blocks.to_be_bytes());
    raw[11] = alignment;
    Oti::deserialize(&raw)
}

#[test]
fn reject_invalid_oti_before_decoder_construction() {
    for oti in [
        raw_oti(1024, 0, 1, 1, 8),
        raw_oti(1024, 128, 0, 1, 8),
        raw_oti(1024, 128, 1, 0, 8),
        raw_oti(1024, 128, 1, 1, 0),
        raw_oti(1024, 127, 1, 1, 8),
        raw_oti(1024, 128, 1, 17, 8),
        raw_oti(1024, 128, 9, 1, 8),
        raw_oti(0, 128, 1, 1, 8),
        raw_oti(11, 128, 1, 1, 8),
        raw_oti((MAX_VIDEO_PAYLOAD + MEDIA_HEADER + 1) as u64, 1280, 2, 1, 8),
        raw_oti((1u64 << 40) - 1, 1280, 1, 1, 8),
        raw_oti(56_404 * 128, 128, 1, 1, 8),
        raw_oti(131_073 * 128, 128, 3, 1, 8),
    ] {
        assert!(
            ObjectDecoder::new(oti, MAX_VIDEO_PAYLOAD).is_err(),
            "{oti:?}"
        );
    }
    assert!(ObjectDecoder::new(raw_oti(65_536 + 13, 1280, 1, 1, 8), MAX_AUDIO_PAYLOAD).is_err());
}

#[test]
fn supported_sender_sizes_and_mtu_parameters_fit_bounds() {
    for limit in [MAX_AUDIO_PAYLOAD, MAX_VIDEO_PAYLOAD] {
        for size in [MEDIA_HEADER, 480, 64 * 1024, limit + MEDIA_HEADER] {
            for symbol in [1100, 1280, 1450, 2740, 8900] {
                let oti = Oti::with_defaults(size as u64, symbol);
                validate_oti(&oti, limit).unwrap();
                check_pending(0, 0, reservation(&oti), limit).unwrap();
            }
        }
    }
}

#[test]
fn full_headers_still_require_exact_symbols_and_valid_payload_ids() {
    let data = media_bytes(300);
    let encoder = raptorq::Encoder::with_defaults(&data, 128);
    let oti = encoder.get_config();
    let source = encoder.get_encoded_packets(2).remove(0);
    for header in [22, 26] {
        let valid = datagram(header, oti, source.clone());
        parse_datagram(valid.clone(), header, MAX_VIDEO_PAYLOAD).unwrap();
        for length in 0..valid.len() {
            assert!(parse_datagram(valid.slice(..length), header, MAX_VIDEO_PAYLOAD).is_err());
        }
        let mut long = valid.to_vec();
        long.push(0);
        assert!(parse_datagram(Bytes::from(long), header, MAX_VIDEO_PAYLOAD).is_err());
        let mut reserved = valid.to_vec();
        reserved[header - 16 + 5] = 1;
        assert!(parse_datagram(Bytes::from(reserved), header, MAX_VIDEO_PAYLOAD).is_err());
        let mut block = valid.to_vec();
        block[header - 4] = oti.source_blocks();
        assert!(parse_datagram(Bytes::from(block), header, MAX_VIDEO_PAYLOAD).is_err());
        // IDs between K and K' denote implicit padding, not wire symbols.
        let padding = EncodingPacket::new(PayloadId::new(0, 3), vec![0; 128]);
        assert!(parse_datagram(datagram(header, oti, padding), header, MAX_VIDEO_PAYLOAD).is_err());
        let repair = EncodingPacket::new(PayloadId::new(0, 0x00ff_ffff), vec![0; 128]);
        parse_datagram(datagram(header, oti, repair), header, MAX_VIDEO_PAYLOAD).unwrap();
    }
}

#[test]
fn decoder_checks_consistency_and_deduplicates_before_retaining() {
    let oti = Oti::with_defaults(1024, 128);
    let mut decoder = ObjectDecoder::new(oti, MAX_VIDEO_PAYLOAD).unwrap();
    let changed = Oti::with_defaults(2048, 128);
    assert!(
        decoder
            .decode(changed, PayloadId::new(0, 0), Bytes::from(vec![0; 128]))
            .is_err()
    );
    assert!(
        decoder
            .decode(oti, PayloadId::new(1, 0), Bytes::from(vec![0; 128]))
            .is_err()
    );
    assert!(
        decoder
            .decode(oti, PayloadId::new(0, 0), Bytes::from(vec![0; 127]))
            .is_err()
    );
    for _ in 0..1000 {
        assert!(
            decoder
                .decode(oti, PayloadId::new(0, 0), Bytes::from(vec![0; 128]))
                .unwrap()
                .is_none()
        );
    }
    assert_eq!(decoder.received.len(), 1);
    assert_eq!(decoder.received_sources, 1);
    // Force the otherwise improbably reached budget boundary without asking
    // the decoder to repeatedly solve intentionally unsatisfiable equations.
    decoder.per_block[0] = 2 * 8 + 32;
    assert!(
        decoder
            .decode(oti, PayloadId::new(0, 100), Bytes::from(vec![0; 128]))
            .is_err()
    );
    assert_eq!(decoder.received.len(), 1);
}

#[test]
fn multi_block_sub_block_repair_reconstructs_exact_bytes() {
    let data = media_bytes(6000);
    let oti = Oti::new(data.len() as u64, 128, 3, 4, 8);
    let encoder = raptorq::Encoder::new(&data, oti);
    let mut decoder = ObjectDecoder::new(oti, MAX_VIDEO_PAYLOAD).unwrap();
    let mut result = None;
    for (i, packet) in encoder.get_encoded_packets(8).into_iter().enumerate() {
        if i % 10 == 0 {
            continue;
        }
        let (id, bytes) = packet.split();
        if let Some(assembled) = decoder.decode(oti, id, Bytes::from(bytes)).unwrap() {
            result = Some(assembled);
            break;
        }
    }
    assert_eq!(result.unwrap(), data);
}

#[test]
fn reconstructed_header_is_checked_before_deserialization() {
    for size in 0..MEDIA_HEADER {
        assert!(media_packet(vec![0xff; size]).is_err());
    }
    let mut data = media_bytes(300);
    media_packet(data.clone()).unwrap();
    data[0] &= 0x7f;
    assert!(media_packet(data.clone()).is_err());
    data[0] |= 0xc0; // config records must use the reliable stream
    assert!(media_packet(data.clone()).is_err());
    data[0] &= !0x40;
    data[11] ^= 1; // declared size differs from reconstructed payload
    assert!(media_packet(data).is_err());
}

#[test]
fn pending_limits_reject_counts_and_bytes_without_overflow() {
    for limit in [MAX_AUDIO_PAYLOAD, MAX_VIDEO_PAYLOAD] {
        check_pending(MAX_PENDING_OBJECTS - 1, 0, 1, limit).unwrap();
        assert!(check_pending(MAX_PENDING_OBJECTS, 0, 1, limit).is_err());
        assert!(check_pending(0, usize::MAX, 1, limit).is_err());
        assert!(check_pending(0, 1, usize::MAX, limit).is_err());
    }
}

#[tokio::test]
async fn reliable_companion_rejects_holes_bad_types_and_large_allocations() {
    assert!(
        read_stream_packet(&mut &[0u8; MEDIA_HEADER][..], MAX_VIDEO_PAYLOAD)
            .await
            .is_err()
    );
    for size in [u32::MAX, MAX_CONFIG_PAYLOAD as u32 + 1] {
        let header = MediaPacketHeader {
            is_config: true,
            is_key: false,
            pts: 0,
            size,
        };
        // No body available: invalid lengths must fail without reading it.
        let raw = header.serialize();
        let err = read_stream_packet(&mut &raw[..], MAX_VIDEO_PAYLOAD)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("config type or length"));
    }
    let codec = CodecPacketHeader {
        codec: 1,
        rotation: 0,
        frame_size: 480,
    }
    .serialize();
    assert!(matches!(
        read_stream_packet(&mut &codec[..], MAX_AUDIO_PAYLOAD)
            .await
            .unwrap(),
        AVPacket::Codec(_)
    ));
    let mut data = media_bytes(3);
    data[0] |= 0x40;
    assert!(matches!(
        read_stream_packet(&mut &data[..], MAX_VIDEO_PAYLOAD)
            .await
            .unwrap(),
        AVPacket::Media(_)
    ));
}

#[test]
fn malformed_datagram_mutation_smoke_test() {
    let encoder = raptorq::Encoder::with_defaults(&media_bytes(1500), 128);
    let valid = datagram(
        26,
        encoder.get_config(),
        encoder.get_encoded_packets(2).remove(0),
    );
    // Deterministic byte/bit mutation, including every OTI field and ID.
    // Only parsing here: large but valid objects should not be allocated.
    for index in 0..valid.len() {
        for bit in 0..8 {
            let mut changed = valid.to_vec();
            changed[index] ^= 1 << bit;
            let _ = parse_datagram(Bytes::from(changed), 26, MAX_VIDEO_PAYLOAD);
        }
    }
}
