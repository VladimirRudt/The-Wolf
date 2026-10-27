//! Fake diagnostic server on the real transport (Unix socket / named pipe), for tests.

use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::task::JoinHandle;

use crate::messaging::PayloadWriter;
use crate::{CommandCode, Endpoint, Message, ServerError};

/// What the server sends on one connection: a response message, then optional raw bytes
/// (the "continuation", e.g. a nettrace stream).
pub(crate) struct Reply {
    pub(crate) message: Message,
    pub(crate) continuation: Vec<u8>,
    /// Instead of answering, hold the connection open this long (a hung runtime).
    pub(crate) silence: Option<Duration>,
}

pub(crate) fn ok(payload: Vec<u8>) -> Reply {
    Reply {
        message: Message {
            command: CommandCode::SERVER_OK,
            payload,
        },
        continuation: Vec::new(),
        silence: None,
    }
}

pub(crate) fn error(code: ServerError) -> Reply {
    Reply {
        message: Message {
            command: CommandCode::SERVER_ERROR,
            payload: PayloadWriter::default().u32(code.0).build(),
        },
        ..ok(Vec::new())
    }
}

/// Reads the request, then says nothing for `duration`.
pub(crate) fn silent(duration: Duration) -> Reply {
    Reply {
        silence: Some(duration),
        ..ok(Vec::new())
    }
}

/// Serves one reply per connection, in order. Returns the requests it received.
pub(crate) fn start(replies: Vec<Reply>) -> (Endpoint, JoinHandle<Vec<Message>>) {
    platform::start(replies)
}

/// Answers one request; the caller then drops (closes) the connection, like the runtime
/// does after a response or at the end of a stream.
async fn serve_one<S: AsyncRead + AsyncWrite + Unpin>(stream: &mut S, reply: &Reply) -> Message {
    let request = Message::read(stream).await.expect("request");
    if let Some(duration) = reply.silence {
        tokio::time::sleep(duration).await;
        return request;
    }
    stream
        .write_all(&reply.message.to_bytes().expect("encode"))
        .await
        .expect("respond");
    stream
        .write_all(&reply.continuation)
        .await
        .expect("continuation");
    stream.flush().await.expect("flush");
    request
}

fn unique_name() -> String {
    static NEXT: AtomicU32 = AtomicU32::new(0);
    format!(
        "wolf-ipc-test-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}

#[cfg(unix)]
mod platform {
    use std::path::PathBuf;

    use super::*;

    /// Deletes the socket file when the server task ends, even if it panics.
    struct SocketFile(PathBuf);

    impl Drop for SocketFile {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    pub(super) fn start(replies: Vec<Reply>) -> (Endpoint, JoinHandle<Vec<Message>>) {
        let path = std::env::temp_dir().join(unique_name());
        let listener = tokio::net::UnixListener::bind(&path).expect("bind");
        let socket_file = SocketFile(path.clone());
        let endpoint = Endpoint { pid: 0, path };
        let server = tokio::spawn(async move {
            let _socket_file = socket_file;
            let mut received = Vec::new();
            for reply in &replies {
                let (mut stream, _) = listener.accept().await.expect("accept");
                received.push(serve_one(&mut stream, reply).await);
            }
            received
        });
        (endpoint, server)
    }
}

#[cfg(windows)]
mod platform {
    use tokio::net::windows::named_pipe::ServerOptions;

    use super::*;

    pub(super) fn start(replies: Vec<Reply>) -> (Endpoint, JoinHandle<Vec<Message>>) {
        let path = format!(r"\\.\pipe\{}", unique_name());
        let mut pipe = ServerOptions::new()
            .first_pipe_instance(true)
            .create(&path)
            .expect("create pipe");
        let endpoint = Endpoint {
            pid: 0,
            path: path.clone().into(),
        };
        let server = tokio::spawn(async move {
            let mut received = Vec::new();
            for reply in &replies {
                pipe.connect().await.expect("connect");
                let mut connected = pipe;
                pipe = ServerOptions::new().create(&path).expect("next instance");
                received.push(serve_one(&mut connected, reply).await);
            }
            received
        });
        (endpoint, server)
    }
}
