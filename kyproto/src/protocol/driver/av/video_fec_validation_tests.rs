// SPDX-License-Identifier: AGPL-3.0-or-later
use super::*;
use fec::tests::{datagram, media_bytes};

#[tokio::test]
async fn video_drains_completed_media_after_late_config_without_another_datagram() {
    let (tx, rx) = mpsc::channel(16);
    let (client_tx, mut client_rx) = mpsc::channel(16);
    tx.send(Ok(RecvMsg::Stream(StreamMsg {
        raw_kypacket_seq: 0,
        raw_group_seq: 1,
        packet: AVPacket::Codec(CodecPacket {
            header: CodecPacketHeader {
                codec: 1,
                rotation: 0,
                frame_size: 0,
            },
        }),
    })))
    .await
    .unwrap();
    for seq in 1..=2 {
        let data = media_bytes(16);
        let encoder = raptorq::Encoder::with_defaults(&data, 128);
        for packet in encoder.get_encoded_packets(0) {
            let (payload_id, data) = packet.split();
            tx.send(Ok(RecvMsg::Datagram(DatagramMsg {
                data: Bytes::from(data),
                raw_kypacket_seq: seq,
                raw_group_seq: 1,
                oti: encoder.get_config(),
                payload_id,
            })))
            .await
            .unwrap();
        }
    }
    tx.send(Ok(RecvMsg::Stream(StreamMsg {
        raw_kypacket_seq: 0,
        raw_group_seq: 1,
        packet: config(),
    })))
    .await
    .unwrap();
    let task = tokio::spawn(VideoUnreliableFecProtocolRecvDriver::process(
        rx,
        client_tx,
        KyArc::new(KyMutex::new(ProtocolStats::default())),
    ));
    for index in 0..4 {
        let packet = tokio::time::timeout(Duration::from_secs(1), client_rx.recv())
            .await
            .expect("completed video remained buffered")
            .unwrap()
            .unwrap();
        match packet {
            AVPacket::Codec(_) => assert_eq!(index, 0),
            AVPacket::Media(media) => assert_eq!(media.header.is_config, index == 1),
            _ => panic!("unexpected video hole"),
        }
    }
    drop(tx);
    assert!(task.await.unwrap().is_ok());
}

fn config() -> AVPacket {
    AVPacket::Media(MediaPacket {
        header: MediaPacketHeader {
            is_config: true,
            is_key: true,
            pts: 0,
            size: 0,
        },
        payload: Bytes::new(),
    })
}

#[test]
fn video_parser_rejects_every_short_header() {
    for size in 0..DATAGRAM_HEADER_SIZE {
        assert!(DatagramMsg::parse(Bytes::from(vec![0; size])).is_err());
    }
}

#[test]
fn video_object_metadata_cannot_change_before_or_after_reassembly() {
    let data = media_bytes(1000);
    let encoder = raptorq::Encoder::with_defaults(&data, 128);
    let oti = encoder.get_config();
    let mut groups = PendingGroups::new();
    let packets = encoder.get_encoded_packets(3);
    let (id, bytes) = packets[0].clone().split();
    groups
        .insert_datagram(1, 1, oti, id.clone(), Bytes::from(bytes.clone()))
        .unwrap();
    let changed = raptorq::ObjectTransmissionInformation::with_defaults(1024, 128);
    assert!(
        groups
            .insert_datagram(1, 1, changed, id.clone(), Bytes::from(bytes.clone()))
            .is_err()
    );
    assert!(
        groups
            .insert_datagram(2, 1, oti, id.clone(), Bytes::from(bytes))
            .is_err()
    );
    for packet in packets {
        let msg = DatagramMsg::parse(datagram(DATAGRAM_HEADER_SIZE, oti, packet)).unwrap();
        groups
            .insert_datagram(1, 1, msg.oti, msg.payload_id, msg.data)
            .unwrap();
    }
    assert!(groups.pending_groups[0].segments.is_empty());
    assert_eq!(groups.pending_groups[0].datagrams.len(), 1);
    assert!(
        groups
            .insert_datagram(1, 1, changed, id, Bytes::from(vec![0; 128]))
            .is_err()
    );
    groups.insert_stream_packet(1, 0, config()).unwrap();
    assert!(groups.insert_stream_packet(1, 0, config()).is_err());
    assert!(matches!(
        groups.take_next_packet(0),
        Action::Packet {
            kypacket_seq: 0,
            ..
        }
    ));
    let Action::Packet {
        packet: AVPacket::Media(packet),
        ..
    } = groups.take_next_packet(1)
    else {
        panic!("media missing");
    };
    assert_eq!(&packet.payload[..], &data[KYPACKET_HEADER_SIZE..]);
    assert_eq!(groups.pending_groups[0].usage(), (0, 0));
}

#[test]
fn video_pending_count_groups_and_byte_reservations_are_bounded() {
    let oti = raptorq::ObjectTransmissionInformation::with_defaults(1000, 128);
    let mut groups = PendingGroups::new();
    for seq in 0..fec::MAX_PENDING_OBJECTS as u64 {
        groups
            .insert_datagram(
                1,
                seq,
                oti,
                raptorq::PayloadId::new(0, 0),
                Bytes::from(vec![0; 128]),
            )
            .unwrap();
    }
    assert!(
        groups
            .insert_datagram(
                1,
                1000,
                oti,
                raptorq::PayloadId::new(0, 0),
                Bytes::from(vec![0; 128])
            )
            .is_err()
    );
    // Removing old segments restores capacity (no monotonically growing charge).
    groups.pending_groups[0].drop_expired_segments(1000);
    groups
        .insert_datagram(
            1,
            1000,
            oti,
            raptorq::PayloadId::new(0, 0),
            Bytes::from(vec![0; 128]),
        )
        .unwrap();
    let mut groups = PendingGroups::new();
    for group in 0..fec::MAX_PENDING_GROUPS as u64 {
        groups.prepare_pending_group(group).unwrap();
    }
    assert!(groups.prepare_pending_group(1000).is_err());

    let large = raptorq::ObjectTransmissionInformation::with_defaults(
        (fec::MAX_VIDEO_PAYLOAD + KYPACKET_HEADER_SIZE) as u64,
        1280,
    );
    let mut groups = PendingGroups::new();
    groups
        .insert_datagram(
            1,
            0,
            large,
            raptorq::PayloadId::new(0, 0),
            Bytes::from(vec![0; 1280]),
        )
        .unwrap();
    assert!(
        groups
            .insert_datagram(
                1,
                1,
                large,
                raptorq::PayloadId::new(0, 0),
                Bytes::from(vec![0; 1280])
            )
            .is_err()
    );
    assert_eq!(groups.pending_groups[0].segments.len(), 1);
}

#[test]
fn video_byte_budget_retires_stale_media_but_preserves_config_and_fresh_media() {
    let oti = raptorq::ObjectTransmissionInformation::with_defaults(
        (fec::MAX_VIDEO_PAYLOAD + KYPACKET_HEADER_SIZE) as u64,
        1280,
    );
    let id = raptorq::PayloadId::new(0, 0);
    let data = Bytes::from(vec![0; usize::from(oti.symbol_size())]);
    let mut groups = PendingGroups::new();
    groups.insert_stream_packet(1, 0, config()).unwrap();
    groups
        .insert_datagram(1, 1, oti, id.clone(), data.clone())
        .unwrap();
    let original_usage = groups.pending_groups[0].usage();
    // Control age explicitly so a slow CI worker cannot turn the fresh-limit
    // assertion into a stale-frame eviction.
    groups.pending_groups[0].segments[0].created = Instant::now() + Duration::from_secs(60);
    assert!(
        groups
            .insert_datagram(1, 2, oti, id.clone(), data.clone())
            .is_err()
    );
    assert_eq!(groups.pending_groups[0].usage(), original_usage);
    assert_eq!(groups.retired_through, None);

    groups.pending_groups[0].segments[0].created = Instant::now() - Duration::from_millis(100);
    groups
        .insert_datagram(1, 2, oti, id.clone(), data.clone())
        .unwrap();
    assert_eq!(groups.pending_groups[0].usage(), original_usage);
    assert!(groups.check_capacity(0).is_ok());
    assert!(groups.pending_groups[0].config_packet.is_ready());
    assert!(!groups.pending_groups[0].contains(1));
    assert!(groups.pending_groups[0].contains(2));
    // A late original cannot recreate the retired frame or charge it twice.
    groups.insert_datagram(1, 1, oti, id, data).unwrap();
    assert_eq!(groups.pending_groups[0].usage(), original_usage);
    let Action::Packet {
        kypacket_seq: 0,
        packet: AVPacket::Media(media),
        fec_stats,
    } = groups.take_next_packet(0)
    else {
        panic!("budget retirement lost the reliable config");
    };
    assert!(media.header.is_config);
    let sources = oti.transfer_length().div_ceil(u64::from(oti.symbol_size()));
    assert_eq!(fec_stats.source_symbols, sources);
    assert_eq!(fec_stats.missing_source_symbols, sources - 1);
    assert_eq!(fec_stats.unrecovered_source_symbols, sources - 1);
    assert_eq!(groups.retired_stats, VideoFecStats::default());
}

#[test]
fn video_retirement_accounts_complete_and_incomplete_frames_once() {
    let encoder = raptorq::Encoder::with_defaults(&media_bytes(1000), 128);
    let oti = encoder.get_config();
    let packets = encoder.get_encoded_packets(3);
    let (id, data) = packets[0].clone().split();
    let data = Bytes::from(data);
    let mut groups = PendingGroups::new();
    // Frame 1 is reconstructed with a missing original; all the other frames
    // remain incomplete while the reliable config is delayed.
    for packet in packets.iter().skip(1) {
        let (id, data) = packet.clone().split();
        groups
            .insert_datagram(1, 1, oti, id, Bytes::from(data))
            .unwrap();
    }
    for seq in 2..=fec::MAX_PENDING_OBJECTS as u64 {
        groups
            .insert_datagram(1, seq, oti, id.clone(), data.clone())
            .unwrap();
    }
    let stale = Instant::now() - Duration::from_millis(100);
    groups.pending_groups[0].datagrams[0].instant = stale;
    groups.pending_groups[0].segments[0].created = stale;
    groups.insert_stream_packet(1, 0, config()).unwrap();
    groups
        .insert_datagram(1, 129, oti, id.clone(), data.clone())
        .unwrap();
    for seq in [1, 2] {
        groups
            .insert_datagram(1, seq, oti, id.clone(), data.clone())
            .unwrap();
        assert!(!groups.pending_groups[0].contains(seq));
    }
    let Action::Packet {
        kypacket_seq: 0,
        fec_stats,
        ..
    } = groups.take_next_packet(0)
    else {
        panic!("config must precede remaining video");
    };
    let sources = oti.transfer_length().div_ceil(u64::from(oti.symbol_size()));
    assert_eq!(fec_stats.source_symbols, 2 * sources);
    assert_eq!(fec_stats.missing_source_symbols, sources);
    assert_eq!(fec_stats.unrecovered_source_symbols, sources - 1);

    // Completing frame 3 resumes delivery after the gap without reporting
    // either retired frame's FEC totals a second time.
    for packet in packets {
        let (id, data) = packet.split();
        groups
            .insert_datagram(1, 3, oti, id, Bytes::from(data))
            .unwrap();
    }
    groups.pending_groups[0].datagrams[0].instant = stale;
    let Action::Packet {
        kypacket_seq: 3,
        fec_stats,
        ..
    } = groups.take_next_packet(1)
    else {
        panic!("video must resume after retired frames");
    };
    assert_eq!(
        fec_stats,
        VideoFecStats {
            source_symbols: sources,
            ..VideoFecStats::default()
        }
    );
}

#[test]
fn video_reassembly_rejects_forged_media_header() {
    let encoder = raptorq::Encoder::with_defaults(&[0; 100], 128);
    let oti = encoder.get_config();
    let (id, bytes) = encoder.get_encoded_packets(0).remove(0).split();
    let mut groups = PendingGroups::new();
    assert!(
        groups
            .insert_datagram(1, 1, oti, id, Bytes::from(bytes))
            .is_err()
    );
    assert!(groups.pending_groups[0].datagrams.is_empty());
}

#[tokio::test]
async fn video_process_returns_protocol_error_not_a_panic_or_hang() {
    let (tx, rx) = mpsc::channel(16);
    let (client_tx, _client_rx) = mpsc::channel(16);
    tx.send(Err(
        DatagramMsg::parse(Bytes::from_static(&[0, 1])).unwrap_err()
    ))
    .await
    .unwrap();
    let result = tokio::time::timeout(
        Duration::from_secs(1),
        VideoUnreliableFecProtocolRecvDriver::process(
            rx,
            client_tx,
            KyArc::new(KyMutex::new(ProtocolStats::default())),
        ),
    )
    .await
    .unwrap();
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("truncated datagram")
    );
}

#[tokio::test]
async fn video_recv_surfaces_error_and_cancels_companion_tasks() {
    let (tx, rx) = mpsc::channel(16);
    let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel::<()>();
    let task = Task::spawn_task(
        async move {
            let _drop_on_cancel = dropped_tx;
            std::future::pending::<()>().await;
        },
        "fec-validation-cancellation-test",
    );
    let mut driver = VideoUnreliableFecProtocolRecvDriver {
        rx_client: rx,
        recv_stream_packets_task: Some(task),
        recv_datagrams_task: None,
        process_task: None,
    };
    tx.send(Err(fec::invalid("test malformed peer")))
        .await
        .unwrap();
    assert!(driver.recv().await.is_err());
    assert!(
        tokio::time::timeout(Duration::from_secs(1), dropped_rx)
            .await
            .unwrap()
            .is_err()
    );
    assert!(driver.recv_stream_packets_task.is_none());
}
