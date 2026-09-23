// SPDX-License-Identifier: AGPL-3.0-or-later
use super::*;
use fec::tests::{datagram, media_bytes};

#[tokio::test]
async fn audio_drains_completed_media_after_late_config_without_another_datagram() {
    let (tx, rx) = mpsc::channel(16);
    let (client_tx, mut client_rx) = mpsc::channel(16);
    tx.send(Ok(RecvMsg::Stream(StreamMsg {
        raw_kypacket_seq: 0,
        packet: AVPacket::Codec(CodecPacket {
            header: CodecPacketHeader {
                codec: 1,
                rotation: 0,
                frame_size: 480,
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
                oti: encoder.get_config(),
                payload_id,
            })))
            .await
            .unwrap();
        }
    }
    tx.send(Ok(RecvMsg::Stream(StreamMsg {
        raw_kypacket_seq: 0,
        packet: AVPacket::Media(MediaPacket {
            header: MediaPacketHeader {
                is_config: true,
                is_key: true,
                pts: 0,
                size: 0,
            },
            payload: Bytes::new(),
        }),
    })))
    .await
    .unwrap();
    let task = tokio::spawn(AudioUnreliableFecProtocolRecvDriver::process(
        rx,
        client_tx,
        KyArc::new(KyMutex::new(ProtocolStats::default())),
    ));
    // Keep the input channel open but send nothing else: a new arrival must
    // not be required to publish media that has already been reconstructed.
    for index in 0..4 {
        let packet = tokio::time::timeout(Duration::from_secs(1), client_rx.recv())
            .await
            .expect("completed audio remained buffered")
            .unwrap()
            .unwrap();
        match packet {
            AVPacket::Codec(_) => assert_eq!(index, 0),
            AVPacket::Media(media) => assert_eq!(media.header.is_config, index == 1),
            _ => panic!("unexpected audio hole"),
        }
    }
    drop(tx);
    assert!(task.await.unwrap().is_ok());
}

#[test]
fn audio_parser_rejects_every_short_header() {
    for size in 0..DATAGRAM_HEADER_SIZE {
        assert!(DatagramMsg::parse(Bytes::from(vec![0; size])).is_err());
    }
}

#[test]
fn audio_repairs_loss_and_rejects_changing_metadata() {
    let data = media_bytes(1500);
    let encoder = raptorq::Encoder::with_defaults(&data, 128);
    let oti = encoder.get_config();
    let mut group = PendingGroup::new();
    let packets = encoder.get_encoded_packets(4);
    let (id, bytes) = packets[0].clone().split();
    group
        .insert_datagram(1, oti, id.clone(), Bytes::from(bytes))
        .unwrap();
    let changed = raptorq::ObjectTransmissionInformation::with_defaults(1024, 128);
    assert!(
        group
            .insert_datagram(1, changed, id.clone(), Bytes::from(vec![0; 128]))
            .is_err()
    );
    // Drop two originals, reorder everything else, include a duplicate.
    for packet in packets
        .into_iter()
        .rev()
        .filter(|p| ![1, 3].contains(&p.payload_id().encoding_symbol_id()))
    {
        let msg = DatagramMsg::parse(datagram(DATAGRAM_HEADER_SIZE, oti, packet)).unwrap();
        group
            .insert_datagram(1, msg.oti, msg.payload_id, msg.data)
            .unwrap();
    }
    assert!(group.segments.is_empty());
    assert_eq!(group.datagrams.len(), 1);
    assert!(
        group
            .insert_datagram(1, changed, id, Bytes::from(vec![0; 128]))
            .is_err()
    );
    let AVPacket::Media(packet) = &group.datagrams[0].packet else {
        panic!("media missing");
    };
    assert_eq!(&packet.payload[..], &data[KYPACKET_HEADER_SIZE..]);
}

#[test]
fn audio_object_budget_and_reconstructed_header_are_checked() {
    let oti = raptorq::ObjectTransmissionInformation::with_defaults(1000, 128);
    let mut group = PendingGroup::new();
    for seq in 0..fec::MAX_PENDING_OBJECTS as u64 {
        group
            .insert_datagram(
                seq,
                oti,
                raptorq::PayloadId::new(0, 0),
                Bytes::from(vec![0; 128]),
            )
            .unwrap();
    }
    assert!(
        group
            .insert_datagram(
                1000,
                oti,
                raptorq::PayloadId::new(0, 0),
                Bytes::from(vec![0; 128])
            )
            .is_err()
    );
    group.drop_expired_segments(1000);
    group
        .insert_datagram(
            1000,
            oti,
            raptorq::PayloadId::new(0, 0),
            Bytes::from(vec![0; 128]),
        )
        .unwrap();
    let encoder = raptorq::Encoder::with_defaults(&[0; 100], 128);
    let (id, bytes) = encoder.get_encoded_packets(0).remove(0).split();
    assert!(
        group
            .insert_datagram(1001, encoder.get_config(), id, Bytes::from(bytes))
            .is_err()
    );
}

#[tokio::test]
async fn audio_process_rejects_invalid_codec_and_reports_parser_errors() {
    for message in [
        Err(DatagramMsg::parse(Bytes::from_static(&[0, 1])).unwrap_err()),
        Ok(RecvMsg::Stream(StreamMsg {
            raw_kypacket_seq: 0,
            packet: AVPacket::Codec(CodecPacket {
                header: CodecPacketHeader {
                    codec: 1,
                    rotation: 0,
                    frame_size: 0,
                },
            }),
        })),
    ] {
        let (tx, rx) = mpsc::channel(16);
        let (client_tx, _client_rx) = mpsc::channel(16);
        tx.send(message).await.unwrap();
        assert!(
            tokio::time::timeout(
                Duration::from_secs(1),
                AudioUnreliableFecProtocolRecvDriver::process(
                    rx,
                    client_tx,
                    KyArc::new(KyMutex::new(ProtocolStats::default()))
                )
            )
            .await
            .unwrap()
            .is_err()
        );
    }
}
