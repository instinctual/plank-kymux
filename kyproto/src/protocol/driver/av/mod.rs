use crate::protocol::ProtocolError;

use bytes::BytesMut;
use kymux_types::av::*;
use kynet::error::ReadExactError;
use kynet::RecvStream;

pub(crate) mod audio_unreliable;
pub(crate) mod audio_unreliable_fec;
pub(crate) mod reliable;
pub(crate) mod video_gopstream;
pub(crate) mod video_unreliable;
pub(crate) mod video_unreliable_fec;

pub(crate) async fn read_packet(recv: &mut RecvStream) -> Result<Option<AVPacket>, ProtocolError> {
    let mut header = [0; AVPacketHeader::SERIALIZED_SIZE];
    let res = recv.read_exact(&mut header).await;
    if let Err(ReadExactError::FinishedEarly(read)) = res {
        if read == 0 {
            return Ok(None);
        }
    }
    res.map_err(ProtocolError::new)?;

    let header = AVPacketHeader::deserialize(&header);
    let packet = match header {
        AVPacketHeader::Media(header) => {
            let mut buf = BytesMut::zeroed(header.size as usize);

            recv.read_exact(&mut buf)
                .await
                .map_err(ProtocolError::new)?;

            AVPacket::Media(MediaPacket {
                header,
                payload: buf.freeze(),
            })
        }
        AVPacketHeader::Codec(header) => AVPacket::Codec(CodecPacket { header }),
        AVPacketHeader::Hole(header) => AVPacket::Hole(HolePacket { header }),
    };

    Ok(Some(packet))
}
