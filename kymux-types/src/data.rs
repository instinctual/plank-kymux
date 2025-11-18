use bytes::Bytes;

#[derive(Debug)]
pub struct DataPacket {
    pub payload: Bytes,
}
