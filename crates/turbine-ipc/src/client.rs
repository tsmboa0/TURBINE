//! IPC client (plan §9.1): connect, send one request, read one response.

use std::path::Path;

use crate::frame::{recv, send, IpcError, Result};
use crate::proto::{Request, Response};

#[cfg(unix)]
pub async fn request(socket_path: &Path, req: Request) -> Result<Response> {
    use tokio::net::UnixStream;
    let mut stream = UnixStream::connect(socket_path).await?;
    send(&mut stream, &req).await?;
    recv::<_, Response>(&mut stream).await?.ok_or(IpcError::Closed)
}

#[cfg(not(unix))]
pub async fn request(_socket_path: &Path, req: Request) -> Result<Response> {
    use tokio::net::TcpStream;
    let mut stream =
        TcpStream::connect(("127.0.0.1", crate::server::FALLBACK_TCP_PORT)).await?;
    send(&mut stream, &req).await?;
    recv::<_, Response>(&mut stream).await?.ok_or(IpcError::Closed)
}
