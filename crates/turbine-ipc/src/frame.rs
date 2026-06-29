//! Length-prefixed bincode framing (plan §9.2): a `u32` little-endian length
//! header followed by the bincode payload, over any async byte stream.

use serde::de::DeserializeOwned;
use serde::Serialize;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Hard cap on a single frame (guards against a corrupt/hostile length header).
const MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug, Error)]
pub enum IpcError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("encode/decode error: {0}")]
    Codec(String),
    #[error("frame too large: {0} bytes")]
    FrameTooLarge(usize),
    #[error("connection closed")]
    Closed,
}

pub type Result<T> = std::result::Result<T, IpcError>;

/// Serialize a value to bincode bytes.
pub fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    bincode::serialize(value).map_err(|e| IpcError::Codec(e.to_string()))
}

/// Deserialize a value from bincode bytes.
pub fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    bincode::deserialize(bytes).map_err(|e| IpcError::Codec(e.to_string()))
}

/// Write one length-prefixed frame and flush.
pub async fn write_frame<W: AsyncWrite + Unpin>(w: &mut W, payload: &[u8]) -> Result<()> {
    if payload.len() > MAX_FRAME_BYTES {
        return Err(IpcError::FrameTooLarge(payload.len()));
    }
    w.write_all(&(payload.len() as u32).to_le_bytes()).await?;
    w.write_all(payload).await?;
    w.flush().await?;
    Ok(())
}

/// Read one length-prefixed frame. Returns `Ok(None)` on a clean EOF (peer closed
/// before sending another frame).
pub async fn read_frame<R: AsyncRead + Unpin>(r: &mut R) -> Result<Option<Vec<u8>>> {
    let mut len_buf = [0u8; 4];
    match r.read_exact(&mut len_buf).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(IpcError::Io(e)),
    }
    let len = u32::from_le_bytes(len_buf) as usize;
    if len > MAX_FRAME_BYTES {
        return Err(IpcError::FrameTooLarge(len));
    }
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf).await?;
    Ok(Some(buf))
}

/// Convenience: encode + write a typed message.
pub async fn send<W: AsyncWrite + Unpin, T: Serialize>(w: &mut W, value: &T) -> Result<()> {
    let bytes = encode(value)?;
    write_frame(w, &bytes).await
}

/// Convenience: read + decode a typed message (None on clean EOF).
pub async fn recv<R: AsyncRead + Unpin, T: DeserializeOwned>(r: &mut R) -> Result<Option<T>> {
    match read_frame(r).await? {
        Some(bytes) => Ok(Some(decode(&bytes)?)),
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn frame_roundtrip() {
        let mut buf: Vec<u8> = Vec::new();
        write_frame(&mut buf, b"hello turbine").await.unwrap();
        let mut cursor = std::io::Cursor::new(buf);
        let got = read_frame(&mut cursor).await.unwrap().unwrap();
        assert_eq!(got, b"hello turbine");
    }

    #[tokio::test]
    async fn clean_eof_is_none() {
        let mut cursor = std::io::Cursor::new(Vec::<u8>::new());
        assert!(read_frame(&mut cursor).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn typed_send_recv() {
        #[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug)]
        struct M {
            a: u32,
            b: String,
        }
        let m = M { a: 7, b: "x".into() };
        let mut buf: Vec<u8> = Vec::new();
        send(&mut buf, &m).await.unwrap();
        let mut cursor = std::io::Cursor::new(buf);
        let got: M = recv(&mut cursor).await.unwrap().unwrap();
        assert_eq!(got, m);
    }
}
