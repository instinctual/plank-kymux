use bytes::Bytes;

#[derive(Debug)]
pub struct MetricsPacket {
    pub payload: Bytes,
}
