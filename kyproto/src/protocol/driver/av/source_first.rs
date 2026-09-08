// SPDX-License-Identifier: AGPL-3.0-or-later
//! Systematic source symbols do not require solving the RaptorQ repair matrix.
//! Use the pinned library's OTI/block partition; retain its repair encoder.
use raptorq::{EncodingPacket, ObjectTransmissionInformation, PayloadId};

pub(super) fn source_packets(data: &[u8], oti: &ObjectTransmissionInformation) -> Vec<EncodingPacket> {
    assert_eq!(data.len() as u64, oti.transfer_length());
    let symbol_size = oti.symbol_size() as usize;
    let mut packets = Vec::with_capacity(data.len().div_ceil(symbol_size));
    for (block_id, (start, end)) in raptorq::calculate_block_offsets(data, oti).into_iter().enumerate() {
        let mut padded = data[start..end.min(data.len())].to_vec();
        padded.resize(end - start, 0);
        let symbol_count = padded.len() / symbol_size;
        // RFC 6330 section 4.4.1.2: each symbol concatenates one subsymbol
        // from each sub-block. This also covers the usual single sub-block.
        let (large, small, large_count, small_count) = raptorq::partition(
            oti.symbol_size() as u32 / oti.symbol_alignment() as u32,
            oti.sub_blocks(),
        );
        let mut symbols: Vec<_> = (0..symbol_count).map(|_| Vec::with_capacity(symbol_size)).collect();
        let mut offset = 0;
        for sub_block in 0..large_count + small_count {
            let width = if sub_block < large_count { large } else { small } as usize
                * oti.symbol_alignment() as usize;
            for symbol in &mut symbols {
                symbol.extend_from_slice(&padded[offset..offset + width]);
                offset += width;
            }
        }
        assert_eq!(offset, padded.len());
        packets.extend(symbols.into_iter().enumerate().map(|(index, symbol)| {
            EncodingPacket::new(PayloadId::new(block_id as u8, index as u32), symbol)
        }));
    }
    packets
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check(size: usize, oti: ObjectTransmissionInformation) {
        let data: Vec<_> = (0..size).map(|i| (i.wrapping_mul(73) ^ (i >> 9)) as u8).collect();
        let sources = source_packets(&data, &oti);
        let encoder = raptorq::Encoder::new(&data, oti);
        let expected: Vec<_> = encoder.get_block_encoders().iter()
            .flat_map(|block| block.source_packets()).collect();
        assert_eq!(sources, expected, "systematic bytes, padding and IDs must match upstream");
        for loss in [0usize, 5, 10, 20] {
            let mut decoder = raptorq::Decoder::new(oti);
            let mut decoded = None;
            for (i, packet) in sources.iter().enumerate() {
                if (i + 1) * loss / 100 != i * loss / 100 { continue; }
                if let Some(result) = decoder.decode(packet.clone()) { decoded = Some(result); }
            }
            let repairs = ((sources.len() as f32 * 0.3).ceil() as u32).max(2);
            for block in encoder.get_block_encoders() {
                for packet in block.repair_packets(0, repairs) {
                    if let Some(result) = decoder.decode(packet) { decoded = Some(result); }
                }
            }
            assert_eq!(decoded.as_deref(), Some(data.as_slice()), "loss={loss}");
        }
    }

    #[test]
    fn systematic_bytes_and_recovery_match_library() {
        for size in [12, 1291, 100_000, 1_100_123, 1_600_013] {
            check(size, ObjectTransmissionInformation::with_defaults(size as u64, 1280));
        }
        // Explicit multiple source blocks/sub-blocks/alignment, unequal partitions.
        check(234_567, ObjectTransmissionInformation::new(234_567, 1280, 3, 3, 8));
        check(40_003, ObjectTransmissionInformation::new(40_003, 1000, 2, 7, 4));
    }
}
