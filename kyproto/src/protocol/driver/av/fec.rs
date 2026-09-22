// SPDX-License-Identifier: AGPL-3.0-or-later

//! Validation boundary for untrusted FEC media. RaptorQ 1.x constructors and
//! decoding routines assume valid RFC 6330 parameters and may panic otherwise.
//! Do not construct a decoder, allocate from an OTI, or deserialize a media
//! header until its corresponding checks here have succeeded.

use crate::ProtocolError;
use bytes::{Bytes, BytesMut};
use kymux_types::av::*;
use raptorq::{Decoder, EncodingPacket, ObjectTransmissionInformation as Oti, PayloadId};
use std::collections::HashSet;
use tokio::io::{AsyncRead, AsyncReadExt};

// The native frame limit plus its 16-byte PLANK per-frame metadata envelope.
pub(super) const MAX_VIDEO_PAYLOAD: usize = 64 * 1024 * 1024 + 16;
pub(super) const MAX_AUDIO_PAYLOAD: usize = 64 * 1024;
pub(super) const MAX_PENDING_OBJECTS: usize = 128;
pub(super) const MAX_PENDING_GROUPS: usize = 32;
const MAX_SOURCE_SYMBOLS: u64 = 131_072;
// RFC 6330 / RaptorQ 1.8's systematic parameter table upper bound.
const MAX_BLOCK_SYMBOLS: u64 = 56_403;
const MEDIA_HEADER: usize = AVPacketHeader::SERIALIZED_SIZE;
const MAX_CONFIG_PAYLOAD: usize = 1024 * 1024;

pub(super) fn invalid(reason: &str) -> ProtocolError {
    ProtocolError(format!("Invalid FEC media: {reason}"))
}

pub(super) fn validate_oti(oti: &Oti, max_payload: usize) -> Result<(), ProtocolError> {
    let length = oti.transfer_length();
    let symbol = u64::from(oti.symbol_size());
    let blocks = u64::from(oti.source_blocks());
    let alignment = u64::from(oti.symbol_alignment());
    let sub_blocks = u64::from(oti.sub_blocks());
    if !(MEDIA_HEADER as u64..=(max_payload + MEDIA_HEADER) as u64).contains(&length) {
        return Err(invalid("object length outside media limit"));
    }
    if symbol == 0 || blocks == 0 || alignment == 0 || sub_blocks == 0 {
        return Err(invalid("zero RaptorQ partition parameter"));
    }
    if symbol % alignment != 0 || sub_blocks > symbol / alignment {
        return Err(invalid("invalid symbol alignment or sub-block partition"));
    }
    let symbols = length.div_ceil(symbol);
    if symbols > MAX_SOURCE_SYMBOLS
        || blocks > symbols
        || symbols.div_ceil(blocks) > MAX_BLOCK_SYMBOLS
    {
        return Err(invalid("source-symbol partition exceeds decoder bounds"));
    }
    Ok(())
}

pub(super) fn source_symbols_for_block(oti: &Oti, block: u8) -> u32 {
    // Only called after validate_oti() and checking the block number.
    let symbols = oti.transfer_length().div_ceil(u64::from(oti.symbol_size())) as u32;
    let (large, small, large_blocks, _) = raptorq::partition(symbols, oti.source_blocks());
    if u32::from(block) < large_blocks {
        large
    } else {
        small
    }
}

fn validate_symbol(oti: &Oti, id: &PayloadId, size: usize) -> Result<(), ProtocolError> {
    if id.source_block_number() >= oti.source_blocks() {
        return Err(invalid("source block outside object"));
    }
    if size != usize::from(oti.symbol_size()) {
        return Err(invalid("symbol length does not match OTI"));
    }
    let sources = source_symbols_for_block(oti, id.source_block_number());
    let extended = raptorq::extended_source_block_symbols(sources);
    if (sources..extended).contains(&id.encoding_symbol_id()) {
        return Err(invalid("padding symbol must not be transmitted"));
    }
    Ok(())
}

pub(super) fn parse_datagram(
    datagram: Bytes,
    header_size: usize,
    max_payload: usize,
) -> Result<(Bytes, Oti, PayloadId), ProtocolError> {
    if datagram.len() < header_size {
        return Err(invalid("truncated datagram header"));
    }
    // The last 16 header bytes contain OTI and payload ID in both media lanes.
    let offset = header_size - 16;
    let raw_oti: &[u8; 12] = datagram[offset..offset + 12].try_into().unwrap();
    if raw_oti[5] != 0 {
        return Err(invalid("nonzero reserved OTI field"));
    }
    let oti = Oti::deserialize(raw_oti);
    validate_oti(&oti, max_payload)?;
    let id = PayloadId::deserialize(datagram[offset + 12..header_size].try_into().unwrap());
    validate_symbol(&oti, &id, datagram.len() - header_size)?;
    Ok((datagram.slice(header_size..), oti, id))
}

// Reserve for padded source/repair/result buffers and per-symbol bookkeeping,
// not just bytes received so far. Solver scratch is additionally bounded by
// MAX_BLOCK_SYMBOLS and the unique-symbol cap in ObjectDecoder::decode(). This
// is an admission budget, not a claim to measure RaptorQ's exact heap usage.
pub(super) fn reservation(oti: &Oti) -> usize {
    let symbols = oti.transfer_length().div_ceil(u64::from(oti.symbol_size())) as usize;
    3 * symbols * usize::from(oti.symbol_size())
        + 128 * symbols
        + 4096 * usize::from(oti.source_blocks())
}

pub(super) fn check_pending(
    objects: usize,
    bytes: usize,
    additional: usize,
    max_payload: usize,
) -> Result<(), ProtocolError> {
    let limit = if max_payload == MAX_VIDEO_PAYLOAD {
        256 * 1024 * 1024
    } else {
        8 * 1024 * 1024
    };
    if objects >= MAX_PENDING_OBJECTS || additional > limit || bytes > limit - additional {
        return Err(invalid("pending receive budget exceeded"));
    }
    Ok(())
}

#[derive(Debug)]
pub(super) struct ObjectDecoder {
    pub(super) oti: Oti,
    decoder: Decoder,
    received: HashSet<(u8, u32)>,
    per_block: Vec<u32>,
    pub(super) received_sources: u64,
}

impl ObjectDecoder {
    pub(super) fn new(oti: Oti, max_payload: usize) -> Result<Self, ProtocolError> {
        validate_oti(&oti, max_payload)?;
        Ok(Self {
            oti,
            decoder: Decoder::new(oti),
            received: HashSet::new(),
            per_block: vec![0; usize::from(oti.source_blocks())],
            received_sources: 0,
        })
    }

    pub(super) fn decode(
        &mut self,
        oti: Oti,
        id: PayloadId,
        data: Bytes,
    ) -> Result<Option<Vec<u8>>, ProtocolError> {
        if oti != self.oti {
            return Err(invalid("OTI changed within one object"));
        }
        validate_symbol(&oti, &id, data.len())?;
        let key = (id.source_block_number(), id.encoding_symbol_id());
        if self.received.contains(&key) {
            return Ok(None);
        }
        let sources = source_symbols_for_block(&oti, id.source_block_number());
        let count = &mut self.per_block[usize::from(id.source_block_number())];
        // Do not let an attacker grow RaptorQ's repair vector or solve matrix
        // indefinitely. This permits far more than the sender's 30% repairs.
        if *count >= 2 * sources + 32 {
            return Err(invalid("too many distinct symbols for one source block"));
        }
        *count += 1;
        self.received.insert(key);
        self.received_sources += u64::from(id.encoding_symbol_id() < sources);
        Ok(self.decoder.decode(EncodingPacket::new(id, data.into())))
    }
}

pub(super) fn media_packet(data: Vec<u8>) -> Result<AVPacket, ProtocolError> {
    if data.len() < MEDIA_HEADER || data[0] & 0x80 == 0 {
        return Err(invalid(
            "reconstructed object is not a complete media packet",
        ));
    }
    let header = MediaPacketHeader::deserialize(&data[..MEDIA_HEADER]);
    if header.is_config || header.size as usize != data.len() - MEDIA_HEADER {
        return Err(invalid("reconstructed media type or length mismatch"));
    }
    Ok(AVPacket::Media(MediaPacket {
        header,
        payload: Bytes::from(data).slice(MEDIA_HEADER..),
    }))
}

// Codec/config records arrive on the companion reliable stream. Validate them
// too: a forged config length must not bypass the FEC receive memory bound.
pub(super) async fn read_stream_packet(
    stream: &mut (impl AsyncRead + Unpin),
    max_payload: usize,
) -> Result<AVPacket, ProtocolError> {
    let mut raw = [0; MEDIA_HEADER];
    stream
        .read_exact(&mut raw)
        .await
        .map_err(ProtocolError::new)?;
    if raw[0] & 0x80 == 0 {
        if raw[..4] == [0; 4] {
            return Err(invalid("unexpected hole record from sender"));
        }
        return Ok(AVPacket::Codec(CodecPacket {
            header: CodecPacketHeader::deserialize(&raw),
        }));
    }
    let header = MediaPacketHeader::deserialize(&raw);
    if !header.is_config || header.size as usize > max_payload.min(MAX_CONFIG_PAYLOAD) {
        return Err(invalid("invalid reliable config type or length"));
    }
    let mut payload = BytesMut::zeroed(header.size as usize);
    stream
        .read_exact(&mut payload)
        .await
        .map_err(ProtocolError::new)?;
    Ok(AVPacket::Media(MediaPacket {
        header,
        payload: payload.freeze(),
    }))
}

#[cfg(test)]
#[path = "fec_tests.rs"]
pub(super) mod tests;
