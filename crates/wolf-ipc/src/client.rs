//! [`DiagnosticsClient`]: the one entry point for talking to a .NET process.
//!
//! Two I/O paths, on purpose:
//! - **Request/response** (ProcessInfo, StopTracing): async, on tokio.
//! - **Streaming** (EventPipe sessions): blocking std I/O, no tokio. A session is started and
//!   read on a dedicated thread with [`EventSession::read_chunk`], so the data path is fully
//!   under our control: no executor, no hidden buffering, one thread per session.

use std::io::{self, Read};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::sync::OnceCell;

use crate::payloads::ProcessInfoVersion;
use crate::{
    CommandCode, Endpoint, IpcError, Message, ProcessInfo, ServerError, SessionConfig, SessionId,
};

/// Upper bound for connecting, sending a command and reading its response.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

/// Talks to one .NET process over its diagnostic endpoint.
///
/// The runtime serves one command per connection (and turns a CollectTracing connection into
/// the event stream), so the client holds no connection: every method opens its own. What it
/// keeps is what it learned about the runtime — the process info and which command versions
/// it supports — so later calls skip the fallback round trips.
///
/// `&self` methods only: share one client between tasks and threads (e.g. in an `Arc`).
#[derive(Debug)]
pub struct DiagnosticsClient {
    endpoint: Endpoint,
    process_info: OnceCell<ProcessInfo>,
    /// Set once the runtime answered UNKNOWN_COMMAND to CollectTracing3.
    collect_tracing3_unsupported: AtomicBool,
}

impl DiagnosticsClient {
    /// Creates a client. Does no I/O; the first method call connects.
    pub fn new(endpoint: Endpoint) -> Self {
        Self {
            endpoint,
            process_info: OnceCell::new(),
            collect_tracing3_unsupported: AtomicBool::new(false),
        }
    }

    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }

    // --- Request/response (async) --------------------------------------------------------

    /// Asks the process about itself; the answer is cached for the client's lifetime.
    ///
    /// Tries ProcessInfo3, then 2, then 1 when an older runtime answers `UNKNOWN_COMMAND`,
    /// like `dotnet-counters` does.
    ///
    /// # Errors
    ///
    /// Connection and protocol errors, or the runtime's error response.
    pub async fn process_info(&self) -> Result<&ProcessInfo, IpcError> {
        self.process_info
            .get_or_try_init(|| self.fetch_process_info())
            .await
    }

    async fn fetch_process_info(&self) -> Result<ProcessInfo, IpcError> {
        for version in ProcessInfoVersion::ALL {
            match self
                .request(version.command(), &[])
                .await?
                .into_ok_payload()
            {
                Ok(payload) => return ProcessInfo::parse(version, &payload),
                Err(IpcError::Server(ServerError::UNKNOWN_COMMAND)) => continue,
                Err(e) => return Err(e),
            }
        }
        Err(IpcError::Server(ServerError::UNKNOWN_COMMAND))
    }

    /// Stops a session. The runtime then flushes the remaining events into the session's
    /// stream and closes it: the thread reading the [`EventSession`] sees end of stream.
    ///
    /// # Errors
    ///
    /// Connection and protocol errors, or the runtime's error response (e.g. unknown session).
    pub async fn stop_session(&self, id: SessionId) -> Result<(), IpcError> {
        self.request(CommandCode::STOP_TRACING, &id.encode())
            .await?
            .into_ok_payload()?;
        Ok(())
    }

    /// Low level: sends any command on a new connection and returns the raw response. For
    /// tools such as golden-file recorders; prefer the typed methods.
    ///
    /// # Errors
    ///
    /// Connection and protocol errors; an error *response* is returned as a [`Message`].
    pub async fn request(&self, command: CommandCode, payload: &[u8]) -> Result<Message, IpcError> {
        let (_stream, response) =
            async_transport::exchange(&self.endpoint, command, payload, REQUEST_TIMEOUT).await?;
        Ok(response)
    }

    // --- Streaming (blocking) ------------------------------------------------------------

    /// Starts an EventPipe session and returns its byte stream.
    ///
    /// **Blocking.** Call it on the thread that will read the session, never inside an async
    /// task. Connecting, sending the request and reading the response are bounded by a 5 s timeout on every
    /// platform; after it, reads wait as long as the stream needs.
    ///
    /// Uses CollectTracing3 when stacks are off and CollectTracing2 otherwise, as `dotnet-trace`
    /// does. A runtime without CollectTracing3 gets CollectTracing2, which records stacks;
    /// that is remembered for later sessions.
    ///
    /// # Errors
    ///
    /// Connection and protocol errors, [`IpcError::Timeout`], or the runtime's error response
    /// (e.g. invalid provider).
    pub fn start_session(&self, config: &SessionConfig) -> Result<EventSession, IpcError> {
        if !config.request_stackwalk && !self.collect_tracing3_unsupported.load(Ordering::Relaxed) {
            match self.try_start_session(CommandCode::COLLECT_TRACING3, config) {
                Err(IpcError::Server(ServerError::UNKNOWN_COMMAND)) => {
                    self.collect_tracing3_unsupported
                        .store(true, Ordering::Relaxed);
                }
                result => return result,
            }
        }
        self.try_start_session(CommandCode::COLLECT_TRACING2, config)
    }

    fn try_start_session(
        &self,
        command: CommandCode,
        config: &SessionConfig,
    ) -> Result<EventSession, IpcError> {
        let (stream, response) = blocking_transport::exchange(
            &self.endpoint,
            command,
            &config.encode(command),
            REQUEST_TIMEOUT,
        )?;
        let id = SessionId::parse(&response.into_ok_payload()?)?;
        Ok(EventSession { id, stream })
    }
}

/// A running EventPipe session: its id and its raw `nettrace` byte stream.
///
/// Read it on one dedicated thread with [`EventSession::read_chunk`] until it returns 0.
/// Keep reading promptly: while nobody reads, events pile up in the runtime's buffer
/// ([`SessionConfig::circular_buffer_mb`]) and are dropped once it is full. The app itself is
/// never blocked.
#[derive(Debug)]
pub struct EventSession {
    id: SessionId,
    stream: blocking_transport::Stream,
}

impl EventSession {
    /// Id to pass to [`DiagnosticsClient::stop_session`].
    pub fn id(&self) -> SessionId {
        self.id
    }

    /// Blocks until the runtime sends bytes, then copies up to `buf.len()` of them into `buf`.
    /// Returns the number of bytes read; **0 means the stream has ended** (session stopped or
    /// process exited). Chunk boundaries are arbitrary: they do not align with nettrace blocks.
    ///
    /// # Errors
    ///
    /// I/O errors other than the runtime closing the stream.
    pub fn read_chunk(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self.stream.read(buf) {
            Err(e) if is_closed_by_peer(&e) => Ok(0),
            other => other,
        }
    }
}

/// The runtime closing the session is the normal end of the stream. std already reports
/// `ERROR_BROKEN_PIPE` (109) as end of file; Windows can also report `ERROR_PIPE_NOT_CONNECTED`.
fn is_closed_by_peer(e: &io::Error) -> bool {
    const ERROR_PIPE_NOT_CONNECTED: i32 = 233;
    cfg!(windows) && e.raw_os_error() == Some(ERROR_PIPE_NOT_CONNECTED)
}

// ---------------------------------------------------------------------------------------------
// Transports. Both expose the same operation:
//
//     exchange(endpoint, command, payload, timeout) -> (Stream, Message)
//
// connect, send one command, read its response, all within `timeout`; then hand back the
// connection too, for commands that keep streaming on it. `async_transport` does it on tokio
// (request/response); `blocking_transport` on plain std (the event stream, D13).
// ---------------------------------------------------------------------------------------------

/// Windows: all instances of the pipe are in use; retry shortly.
#[cfg(windows)]
const ERROR_PIPE_BUSY: i32 = 231;
/// Pause between connection attempts while the pipe is busy.
#[cfg(windows)]
const PIPE_BUSY_RETRY: Duration = Duration::from_millis(20);

/// Request/response connections (tokio).
mod async_transport {
    use super::*;

    #[cfg(unix)]
    pub(super) type Stream = tokio::net::UnixStream;
    #[cfg(windows)]
    pub(super) type Stream = tokio::net::windows::named_pipe::NamedPipeClient;

    pub(super) async fn exchange(
        endpoint: &Endpoint,
        command: CommandCode,
        payload: &[u8],
        timeout: Duration,
    ) -> Result<(Stream, Message), IpcError> {
        tokio::time::timeout(timeout, async {
            let mut stream = connect(endpoint).await?;
            Message::write(&mut stream, command, payload).await?;
            let response = Message::read(&mut stream).await?;
            Ok((stream, response))
        })
        .await
        .map_err(|_| IpcError::Timeout)?
    }

    #[cfg(unix)]
    async fn connect(endpoint: &Endpoint) -> io::Result<Stream> {
        Stream::connect(&endpoint.path).await
    }

    /// Retries while all pipe instances are busy; `exchange`'s timeout bounds it.
    #[cfg(windows)]
    async fn connect(endpoint: &Endpoint) -> io::Result<Stream> {
        use tokio::net::windows::named_pipe::ClientOptions;
        loop {
            match ClientOptions::new().open(&endpoint.path) {
                Err(e) if e.raw_os_error() == Some(ERROR_PIPE_BUSY) => {
                    tokio::time::sleep(PIPE_BUSY_RETRY).await;
                }
                result => return result,
            }
        }
    }
}

/// Streaming connections (plain std, blocking). After `exchange` the connection has no
/// timeouts left: it is the event stream. Each platform bounds the response wait its own way.
///
/// The request is written without a deadline on both platforms: it is ~100 bytes and fits the
/// socket/pipe buffer, so writing never waits for the runtime.
mod blocking_transport {
    use super::*;

    pub(super) use platform::{Stream, exchange};

    /// Unix: a socket read timeout bounds the wait for the response.
    #[cfg(unix)]
    mod platform {
        use super::*;

        pub(in super::super) type Stream = std::os::unix::net::UnixStream;

        pub(in super::super) fn exchange(
            endpoint: &Endpoint,
            command: CommandCode,
            payload: &[u8],
            timeout: Duration,
        ) -> Result<(Stream, Message), IpcError> {
            // A local socket connects or fails immediately.
            let mut stream = Stream::connect(&endpoint.path)?;
            Message::write_blocking(&mut stream, command, payload)?;
            stream.set_read_timeout(Some(timeout))?;
            let response = Message::read_blocking(&mut stream).map_err(|e| match e {
                // A std read timeout surfaces as `WouldBlock` (or `TimedOut` on some systems).
                IpcError::Io(io)
                    if matches!(
                        io.kind(),
                        io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                    ) =>
                {
                    IpcError::Timeout
                }
                other => other,
            })?;
            stream.set_read_timeout(None)?;
            Ok((stream, response))
        }
    }

    /// Windows: std has no timeouts for pipe I/O. Instead, before the (blocking) read, poll the
    /// pipe with `PeekNamedPipe` until the whole response has arrived or the deadline passes.
    /// Peeking does not consume data, and it is only used while waiting for the response.
    #[cfg(windows)]
    mod platform {
        use std::ffi::c_void;
        use std::os::windows::io::AsRawHandle;
        use std::time::Instant;

        use super::*;
        use crate::messaging::HEADER_SIZE;

        /// A named pipe opened as a plain (non-overlapped) file: blocking reads and writes.
        pub(in super::super) type Stream = std::fs::File;

        /// Pause between checks while waiting for the response.
        const POLL_INTERVAL: Duration = Duration::from_millis(5);
        const ERROR_BROKEN_PIPE: i32 = 109;
        const ERROR_PIPE_NOT_CONNECTED: i32 = 233;

        #[link(name = "kernel32")]
        unsafe extern "system" {
            /// <https://learn.microsoft.com/windows/win32/api/namedpipeapi/nf-namedpipeapi-peeknamedpipe>
            fn PeekNamedPipe(
                pipe: *mut c_void,
                buffer: *mut c_void,
                buffer_size: u32,
                bytes_read: *mut u32,
                total_bytes_available: *mut u32,
                bytes_left_this_message: *mut u32,
            ) -> i32;
        }

        pub(in super::super) fn exchange(
            endpoint: &Endpoint,
            command: CommandCode,
            payload: &[u8],
            timeout: Duration,
        ) -> Result<(Stream, Message), IpcError> {
            let deadline = Instant::now() + timeout;
            let mut stream = connect(endpoint, deadline)?;
            Message::write_blocking(&mut stream, command, payload)?;
            wait_for_message(&stream, deadline)?;
            let response = Message::read_blocking(&mut stream)?;
            Ok((stream, response))
        }

        /// Opens the pipe, retrying while all its instances are busy, until `deadline`.
        fn connect(endpoint: &Endpoint, deadline: Instant) -> io::Result<Stream> {
            loop {
                match std::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(&endpoint.path)
                {
                    Err(e)
                        if e.raw_os_error() == Some(ERROR_PIPE_BUSY)
                            && Instant::now() < deadline =>
                    {
                        std::thread::sleep(PIPE_BUSY_RETRY);
                    }
                    result => return result,
                }
            }
        }

        /// Waits until a whole message (header plus the payload it announces) can be read
        /// without blocking.
        fn wait_for_message(stream: &Stream, deadline: Instant) -> Result<(), IpcError> {
            if !wait_until_available(stream, HEADER_SIZE, deadline)? {
                return Ok(());
            }
            let mut header = [0u8; HEADER_SIZE];
            peek(stream, &mut header)?;
            let (_, payload_len) = Message::parse_header(&header)?;
            wait_until_available(stream, HEADER_SIZE + payload_len, deadline)?;
            Ok(())
        }

        /// `Ok(true)` once `len` bytes are buffered. `Ok(false)` if the runtime closed the pipe:
        /// stop waiting, the read then returns what is left (std reads a closed pipe as EOF).
        fn wait_until_available(
            stream: &Stream,
            len: usize,
            deadline: Instant,
        ) -> Result<bool, IpcError> {
            loop {
                match peek(stream, &mut []) {
                    Ok(available) if available >= len => return Ok(true),
                    Ok(_) if Instant::now() >= deadline => return Err(IpcError::Timeout),
                    Ok(_) => std::thread::sleep(POLL_INTERVAL),
                    Err(e)
                        if matches!(
                            e.raw_os_error(),
                            Some(ERROR_BROKEN_PIPE | ERROR_PIPE_NOT_CONNECTED)
                        ) =>
                    {
                        return Ok(false);
                    }
                    Err(e) => return Err(e.into()),
                }
            }
        }

        /// Copies up to `buf.len()` buffered bytes into `buf` without consuming them; returns
        /// the total number of bytes buffered in the pipe.
        fn peek(stream: &Stream, buf: &mut [u8]) -> io::Result<usize> {
            let buffer = if buf.is_empty() {
                std::ptr::null_mut()
            } else {
                buf.as_mut_ptr().cast()
            };
            let buffer_size = u32::try_from(buf.len()).unwrap_or(u32::MAX);
            let mut available = 0u32;
            // SAFETY: `stream` owns an open pipe handle that outlives the call; `buffer` is
            // null with size 0 or points to `buf`, writable for `buffer_size` bytes;
            // `available` is a live local; the two other out-pointers are optional (null).
            let ok = unsafe {
                PeekNamedPipe(
                    stream.as_raw_handle(),
                    buffer,
                    buffer_size,
                    std::ptr::null_mut(),
                    &mut available,
                    std::ptr::null_mut(),
                )
            };
            if ok == 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(usize::try_from(available).unwrap_or(usize::MAX))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::messaging::PayloadWriter;
    use crate::payloads::tests::{gc_provider, v2_payload, v3_payload};
    use crate::test_server::{self, Reply, error, ok};

    fn commands(requests: &[Message]) -> Vec<CommandCode> {
        requests.iter().map(|m| m.command).collect()
    }

    fn session_ok(id: u64) -> Reply {
        ok(SessionId(id).encode())
    }

    /// Reads a session to its end in small chunks, as a streaming thread would.
    fn read_to_end(session: &mut EventSession) -> Vec<u8> {
        let mut bytes = Vec::new();
        let mut chunk = [0u8; 4];
        loop {
            match session.read_chunk(&mut chunk).expect("read") {
                0 => return bytes,
                n => bytes.extend_from_slice(&chunk[..n]),
            }
        }
    }

    #[tokio::test]
    async fn process_info_falls_back_when_runtime_does_not_know_v3() {
        let (endpoint, server) =
            test_server::start(vec![error(ServerError::UNKNOWN_COMMAND), ok(v2_payload())]);
        let client = DiagnosticsClient::new(endpoint);

        let info = client.process_info().await.expect("process info");
        assert_eq!(info.clr_product_version.as_deref(), Some("8.0.1+abc"));
        assert_eq!(info.runtime_identifier, None);
        assert_eq!(
            commands(&server.await.expect("server")),
            [CommandCode::PROCESS_INFO3, CommandCode::PROCESS_INFO2]
        );
    }

    #[tokio::test]
    async fn process_info_is_cached() {
        let (endpoint, server) = test_server::start(vec![ok(v3_payload())]);
        let client = DiagnosticsClient::new(endpoint);

        let first = client.process_info().await.expect("first").clone();
        let second = client.process_info().await.expect("second");
        assert_eq!(&first, second);
        assert_eq!(server.await.expect("server").len(), 1);
    }

    #[tokio::test]
    async fn other_server_errors_are_returned() {
        let (endpoint, server) = test_server::start(vec![error(ServerError::NOT_SUPPORTED)]);
        let client = DiagnosticsClient::new(endpoint);

        let result = client.process_info().await;
        assert!(
            matches!(result, Err(IpcError::Server(ServerError::NOT_SUPPORTED))),
            "{result:?}"
        );
        assert_eq!(server.await.expect("server").len(), 1);
    }

    // The streaming side is blocking, so these tests run it on a blocking thread while the
    // fake server runs on the tokio runtime — the same split as in production.

    #[tokio::test(flavor = "multi_thread")]
    async fn session_streams_bytes_after_the_response() {
        let (endpoint, server) = test_server::start(vec![Reply {
            continuation: b"Nettrace...".to_vec(),
            ..session_ok(42)
        }]);

        let (id, bytes) = tokio::task::spawn_blocking(move || {
            let client = DiagnosticsClient::new(endpoint);
            let mut session = client
                .start_session(&SessionConfig::new(vec![gc_provider()]))
                .expect("start");
            (session.id(), read_to_end(&mut session))
        })
        .await
        .expect("streaming thread");

        assert_eq!(id, SessionId(42));
        assert_eq!(bytes, b"Nettrace...");
        assert_eq!(
            commands(&server.await.expect("server")),
            [CommandCode::COLLECT_TRACING3]
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn collect_tracing3_fallback_is_remembered() {
        let (endpoint, server) = test_server::start(vec![
            error(ServerError::UNKNOWN_COMMAND),
            session_ok(7),
            session_ok(8),
        ]);

        let ids = tokio::task::spawn_blocking(move || {
            let client = DiagnosticsClient::new(endpoint);
            let config = SessionConfig::new(vec![gc_provider()]);
            let first = client.start_session(&config).expect("first").id();
            let second = client.start_session(&config).expect("second").id();
            (first, second)
        })
        .await
        .expect("streaming thread");

        assert_eq!(ids, (SessionId(7), SessionId(8)));
        assert_eq!(
            commands(&server.await.expect("server")),
            [
                CommandCode::COLLECT_TRACING3,
                CommandCode::COLLECT_TRACING2,
                CommandCode::COLLECT_TRACING2
            ]
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn blocking_exchange_times_out_when_runtime_never_answers() {
        let (endpoint, _server) =
            test_server::start(vec![test_server::silent(Duration::from_secs(3))]);

        let (result, elapsed) = tokio::task::spawn_blocking(move || {
            let started = std::time::Instant::now();
            let result = blocking_transport::exchange(
                &endpoint,
                CommandCode::COLLECT_TRACING3,
                &[],
                Duration::from_millis(200),
            );
            (result.map(|_| ()), started.elapsed())
        })
        .await
        .expect("streaming thread");

        assert!(matches!(result, Err(IpcError::Timeout)), "{result:?}");
        assert!(elapsed < Duration::from_secs(2), "took {elapsed:?}");
    }

    #[tokio::test]
    async fn stop_sends_session_id() {
        let (endpoint, server) = test_server::start(vec![session_ok(42)]);
        let client = DiagnosticsClient::new(endpoint);

        client.stop_session(SessionId(42)).await.expect("stop");
        let requests = server.await.expect("server");
        assert_eq!(requests[0].command, CommandCode::STOP_TRACING);
        assert_eq!(
            requests[0].payload,
            PayloadWriter::default().u64(42).build()
        );
    }
}
