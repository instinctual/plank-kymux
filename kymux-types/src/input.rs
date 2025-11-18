use bytes::Bytes;

#[derive(Debug)]
pub struct InputPacket {
    pub type_: u8,
    pub payload: Bytes,
}
