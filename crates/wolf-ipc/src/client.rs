use std::time::Duration;

use crate::process_info::ProcessInfoVersion;
use crate::{CommandCode, Endpoint, IpcError, Message, ProcessInfo, ServerError, transport};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

/// Sends one command on a new connection and returns the runtime's response.
pub async fn request(
    endpoint: &Endpoint,
    command: CommandCode,
    payload: &[u8],
) -> Result<Message, IpcError> {
    tokio::time::timeout(REQUEST_TIMEOUT, async {
        let mut stream = transport::connect(endpoint).await?;
        Message::write(&mut stream, command, payload).await?;
        Message::read(&mut stream).await
    })
    .await
    .map_err(|_| IpcError::Timeout)?
}

/// Asks a .NET process about itself. Tries the newest ProcessInfo command first and falls
/// back when an older runtime answers `UNKNOWN_COMMAND`, like `dotnet-counters` does.
pub async fn process_info(endpoint: &Endpoint) -> Result<ProcessInfo, IpcError> {
    for (command, version) in [
        (CommandCode::PROCESS_INFO3, ProcessInfoVersion::V3),
        (CommandCode::PROCESS_INFO2, ProcessInfoVersion::V2),
        (CommandCode::PROCESS_INFO, ProcessInfoVersion::V1),
    ] {
        match request(endpoint, command, &[]).await?.into_ok_payload() {
            Ok(payload) => return ProcessInfo::parse(version, &payload),
            Err(IpcError::Server(ServerError::UNKNOWN_COMMAND)) => continue,
            Err(e) => return Err(e),
        }
    }
    Err(IpcError::Server(ServerError::UNKNOWN_COMMAND))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU32, Ordering};

    use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
    use tokio::task::JoinHandle;

    use super::*;
    use crate::process_info::tests::v2_payload;
    use crate::protocol::test_support::PayloadBuilder;

    fn ok(payload: Vec<u8>) -> Message {
        Message {
            command: CommandCode::SERVER_OK,
            payload,
        }
    }

    fn error(code: ServerError) -> Message {
        Message {
            command: CommandCode::SERVER_ERROR,
            payload: PayloadBuilder::default().u32(code.0).build(),
        }
    }

    /// Answers one request per connection, then waits for the client to hang up.
    async fn serve_one<S: AsyncRead + AsyncWrite + Unpin>(
        stream: &mut S,
        response: &Message,
    ) -> CommandCode {
        let request = Message::read(stream).await.expect("request");
        stream
            .write_all(&response.to_bytes().expect("encode"))
            .await
            .expect("respond");
        let _ = stream.read(&mut [0]).await;
        request.command
    }

    fn unique_name() -> String {
        static NEXT: AtomicU32 = AtomicU32::new(0);
        format!(
            "wolf-ipc-test-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        )
    }

    /// Fake diagnostic server on the real transport. Returns the commands it received.
    #[cfg(unix)]
    fn fake_server(responses: Vec<Message>) -> (Endpoint, JoinHandle<Vec<CommandCode>>) {
        let path = std::env::temp_dir().join(unique_name());
        let listener = tokio::net::UnixListener::bind(&path).expect("bind");
        let endpoint = Endpoint { pid: 0, path };
        let server = tokio::spawn(async move {
            let mut received = Vec::new();
            for response in &responses {
                let (mut stream, _) = listener.accept().await.expect("accept");
                received.push(serve_one(&mut stream, response).await);
            }
            received
        });
        (endpoint, server)
    }

    #[cfg(windows)]
    fn fake_server(responses: Vec<Message>) -> (Endpoint, JoinHandle<Vec<CommandCode>>) {
        use tokio::net::windows::named_pipe::ServerOptions;

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
            for response in &responses {
                pipe.connect().await.expect("connect");
                let mut connected = pipe;
                pipe = ServerOptions::new().create(&path).expect("next instance");
                received.push(serve_one(&mut connected, response).await);
            }
            received
        });
        (endpoint, server)
    }

    #[tokio::test]
    async fn falls_back_when_runtime_does_not_know_processinfo3() {
        let (endpoint, server) =
            fake_server(vec![error(ServerError::UNKNOWN_COMMAND), ok(v2_payload())]);

        let info = process_info(&endpoint).await.expect("process info");
        assert_eq!(info.clr_product_version.as_deref(), Some("8.0.1+abc"));
        assert_eq!(info.runtime_identifier, None);
        assert_eq!(
            server.await.expect("server"),
            [CommandCode::PROCESS_INFO3, CommandCode::PROCESS_INFO2]
        );
    }

    #[tokio::test]
    async fn other_server_errors_are_returned() {
        let (endpoint, server) = fake_server(vec![error(ServerError::NOT_SUPPORTED)]);

        let result = process_info(&endpoint).await;
        assert!(
            matches!(result, Err(IpcError::Server(ServerError::NOT_SUPPORTED))),
            "{result:?}"
        );
        assert_eq!(server.await.expect("server"), [CommandCode::PROCESS_INFO3]);
    }
}
