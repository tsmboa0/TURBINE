//! IPC server (plan §9.2). Pure transport: each request is forwarded to the daemon
//! over a command channel (with a oneshot reply), keeping engine/business logic out
//! of this crate. Primary transport is a Unix domain socket; non-UNIX targets fall
//! back to a loopback TCP port.

use std::path::Path;
use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{mpsc, oneshot, Notify};
use tracing::{debug, warn};

use crate::frame::{recv, send, Result};
use crate::proto::{Request, Response};

/// Channel the server uses to hand requests to the daemon for processing.
pub type CommandTx = mpsc::Sender<(Request, oneshot::Sender<Response>)>;

/// Loopback TCP port used as the IPC transport on non-UNIX platforms.
#[cfg(not(unix))]
pub const FALLBACK_TCP_PORT: u16 = 47900;

/// Serve one connection: decode requests, dispatch to the daemon, write replies,
/// until the peer closes. Multiple requests per connection are supported.
async fn handle_conn<S>(mut stream: S, commands: CommandTx) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    while let Some(req) = recv::<_, Request>(&mut stream).await? {
        let (tx, rx) = oneshot::channel();
        if commands.send((req, tx)).await.is_err() {
            break; // daemon command loop gone
        }
        match rx.await {
            Ok(resp) => send(&mut stream, &resp).await?,
            Err(_) => break, // daemon dropped the responder
        }
    }
    Ok(())
}

#[cfg(unix)]
pub async fn serve(socket_path: &Path, commands: CommandTx, shutdown: Arc<Notify>) -> Result<()> {
    use tokio::net::UnixListener;

    // Clear a stale socket from a previous (crashed) run before binding.
    let _ = std::fs::remove_file(socket_path);
    let listener = UnixListener::bind(socket_path)?;
    tracing::info!(path = %socket_path.display(), "IPC listening (unix socket)");

    loop {
        tokio::select! {
            _ = shutdown.notified() => break,
            accept = listener.accept() => match accept {
                Ok((stream, _)) => {
                    let cmds = commands.clone();
                    tokio::spawn(async move {
                        if let Err(e) = handle_conn(stream, cmds).await {
                            debug!("ipc connection ended: {e}");
                        }
                    });
                }
                Err(e) => warn!("ipc accept error: {e}"),
            },
        }
    }

    let _ = std::fs::remove_file(socket_path);
    Ok(())
}

#[cfg(not(unix))]
pub async fn serve(_socket_path: &Path, commands: CommandTx, shutdown: Arc<Notify>) -> Result<()> {
    use tokio::net::TcpListener;

    let listener = TcpListener::bind(("127.0.0.1", FALLBACK_TCP_PORT)).await?;
    tracing::info!(port = FALLBACK_TCP_PORT, "IPC listening (loopback tcp fallback)");

    loop {
        tokio::select! {
            _ = shutdown.notified() => break,
            accept = listener.accept() => match accept {
                Ok((stream, _)) => {
                    let cmds = commands.clone();
                    tokio::spawn(async move {
                        if let Err(e) = handle_conn(stream, cmds).await {
                            debug!("ipc connection ended: {e}");
                        }
                    });
                }
                Err(e) => warn!("ipc accept error: {e}"),
            },
        }
    }
    Ok(())
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use crate::client;

    #[tokio::test]
    async fn server_client_roundtrip() {
        let path = std::env::temp_dir().join(format!("turbine-test-{}.sock", std::process::id()));
        let (cmd_tx, mut cmd_rx) =
            mpsc::channel::<(Request, oneshot::Sender<Response>)>(8);
        let shutdown = Arc::new(Notify::new());

        // Minimal daemon: reply to Ping with Pong, Status with a snapshot.
        tokio::spawn(async move {
            while let Some((req, resp)) = cmd_rx.recv().await {
                let r = match req {
                    Request::Ping => Response::Pong,
                    Request::Stop => Response::Ack { message: "stopping".into() },
                    _ => Response::Error { message: "unsupported".into() },
                };
                let _ = resp.send(r);
            }
        });

        let srv_path = path.clone();
        let srv_shutdown = shutdown.clone();
        let server = tokio::spawn(async move { serve(&srv_path, cmd_tx, srv_shutdown).await });

        // Give the listener a moment to bind.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let resp = client::request(&path, Request::Ping).await.unwrap();
        assert!(matches!(resp, Response::Pong));

        let resp = client::request(&path, Request::Stop).await.unwrap();
        assert!(matches!(resp, Response::Ack { .. }));

        shutdown.notify_waiters();
        let _ = server.await;
    }
}
