// SPDX-License-Identifier: AGPL-3.0-or-later
use super::*;
use fec::tests::{datagram, media_bytes};

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
