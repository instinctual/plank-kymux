// SPDX-License-Identifier: AGPL-3.0-or-later
//! Systematic source symbols do not require solving the RaptorQ repair matrix.
//! Use the pinned library's OTI/block partition; retain its repair encoder.
use raptorq::{EncodingPacket, ObjectTransmissionInformation, PayloadId};

/// Produce systematic packets lazily. Only the current symbol is allocated;
/// neither a padded frame copy nor a frame-sized packet list is needed before
/// the first datagram can be submitted.
pub(super) fn source_packets<'a>(
    data: &'a [u8],
    oti: &ObjectTransmissionInformation,
) -> impl Iterator<Item = EncodingPacket> + 'a {
    assert_eq!(data.len() as u64, oti.transfer_length());
    let symbol_size = oti.symbol_size() as usize;
    let alignment = oti.symbol_alignment() as usize;
    let (large, small, large_count, small_count) = raptorq::partition(
        oti.symbol_size() as u32 / oti.symbol_alignment() as u32,
        oti.sub_blocks(),
    );
    raptorq::calculate_block_offsets(data, oti)
        .into_iter()
        .enumerate()
        .flat_map(move |(block_id, (start, end))| {
            let block = &data[start..end.min(data.len())];
            let symbol_count = (end - start) / symbol_size;
            (0..symbol_count).map(move |index| {
                let mut symbol = Vec::with_capacity(symbol_size);
                let mut offset = 0;
                // RFC 6330 section 4.4.1.2: concatenate one subsymbol from
                // each sub-block, padding only bytes beyond the input. The
                // common single-sub-block case is one contiguous copy.
                for sub_block in 0..large_count + small_count {
                    let width = if sub_block < large_count {
                        large
                    } else {
                        small
                    } as usize
                        * alignment;
                    let source_start = offset + index * width;
                    let source_end = (source_start + width).min(block.len());
                    let padded_end = symbol.len() + width;
                    if source_start < source_end {
                        symbol.extend_from_slice(&block[source_start..source_end]);
                    }
                    symbol.resize(padded_end, 0);
                    offset += symbol_count * width;
                }
                EncodingPacket::new(PayloadId::new(block_id as u8, index as u32), symbol)
            })
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check(size: usize, oti: ObjectTransmissionInformation) {
        let data: Vec<_> = (0..size)
            .map(|i| (i.wrapping_mul(73) ^ (i >> 9)) as u8)
            .collect();
        let sources: Vec<_> = source_packets(&data, &oti).collect();
        let encoder = raptorq::Encoder::new(&data, oti);
        let expected: Vec<_> = encoder
            .get_block_encoders()
            .iter()
            .flat_map(|block| block.source_packets())
            .collect();
        assert_eq!(
            sources, expected,
            "systematic bytes, padding and IDs must match upstream"
        );
        for loss in [0usize, 5, 10, 20] {
            let mut decoder = raptorq::Decoder::new(oti);
            let mut decoded = None;
            for (i, packet) in sources.iter().enumerate() {
                if (i + 1) * loss / 100 != i * loss / 100 {
                    continue;
                }
                if let Some(result) = decoder.decode(packet.clone()) {
                    decoded = Some(result);
                }
            }
            let repairs = ((sources.len() as f32 * 0.3).ceil() as u32).max(2);
            for block in encoder.get_block_encoders() {
                for packet in block.repair_packets(0, repairs) {
                    if let Some(result) = decoder.decode(packet) {
                        decoded = Some(result);
                    }
                }
            }
            assert_eq!(decoded.as_deref(), Some(data.as_slice()), "loss={loss}");
        }
    }

    #[test]
    fn systematic_bytes_and_recovery_match_library() {
        for size in [12, 1291, 100_000, 1_100_123, 1_600_013] {
            check(
                size,
                ObjectTransmissionInformation::with_defaults(size as u64, 1280),
            );
        }
        // Explicit multiple source blocks/sub-blocks/alignment, unequal partitions.
        check(
            234_567,
            ObjectTransmissionInformation::new(234_567, 1280, 3, 3, 8),
        );
        check(
            40_003,
            ObjectTransmissionInformation::new(40_003, 1000, 2, 7, 4),
        );
    }

    #[test]
    fn symbol_boundaries_and_subblock_padding_match_library() {
        for symbol_size in [64u16, 1104, 1280, 1448, 2736, 8896] {
            for size in [
                1,
                symbol_size as usize - 1,
                symbol_size as usize,
                symbol_size as usize + 1,
                17 * symbol_size as usize - 7,
            ] {
                let data: Vec<_> = (0..size).map(|i| (i * 17) as u8).collect();
                for sub_blocks in [1, 3, 8] {
                    let oti = ObjectTransmissionInformation::new(
                        size as u64,
                        symbol_size,
                        1,
                        sub_blocks,
                        8,
                    );
                    let encoder = raptorq::Encoder::new(&data, oti);
                    assert!(
                        source_packets(&data, &oti)
                            .eq(encoder.get_block_encoders()[0].source_packets()),
                        "size={size} symbol={symbol_size} sub_blocks={sub_blocks}"
                    );
                }
            }
        }
    }

    #[test]
    fn loss_and_reordering_cover_originals_and_repairs() {
        let size = 234_567;
        let data: Vec<_> = (0..size).map(|i| (i * 31 + 17) as u8).collect();
        for oti in [
            ObjectTransmissionInformation::with_defaults(size as u64, 1280),
            ObjectTransmissionInformation::new(size as u64, 1280, 3, 3, 8),
        ] {
            let encoder = raptorq::Encoder::new(&data, oti);
            let mut packets: Vec<_> = source_packets(&data, &oti).collect();
            let repairs = ((packets.len() as f32 * 0.3).ceil() as u32).max(2);
            for block in encoder.get_block_encoders() {
                packets.extend(block.repair_packets(0, repairs));
            }
            // Compare every wire symbol, not just reconstructed payloads. The old
            // library API interleaves originals/repairs per block; we submit all
            // originals first, so normalize order only for this equality check.
            let mut expected: Vec<_> = encoder
                .get_encoded_packets(repairs)
                .iter()
                .map(EncodingPacket::serialize)
                .collect();
            let mut actual: Vec<_> = packets.iter().map(EncodingPacket::serialize).collect();
            actual.sort_unstable();
            expected.sort_unstable();
            assert_eq!(actual, expected);

            for loss in [0, 5, 10, 20] {
                let mut decoder = raptorq::Decoder::new(oti);
                let mut recovered = None;
                for (i, packet) in packets.iter().enumerate().rev() {
                    if (i + 1) * loss / 100 != i * loss / 100 {
                        continue;
                    }
                    if let Some(bytes) = decoder.decode(packet.clone()) {
                        recovered = Some(bytes);
                        break;
                    }
                }
                assert_eq!(recovered.as_deref(), Some(data.as_slice()), "loss={loss}");
            }
        }
    }
}
