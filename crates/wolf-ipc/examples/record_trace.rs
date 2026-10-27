//! Records an EventPipe session with GC events to a `.nettrace` file:
//! `cargo run -p wolf-ipc --example record_trace -- <pid> <seconds> <out-file>`.
//!
//! Shows the intended split: a plain thread starts the session and reads chunks; the async
//! side decides when to stop.

use std::error::Error;
use std::io::Write;
use std::sync::Arc;
use std::sync::mpsc;
use std::time::Duration;

use wolf_ipc::{DiagnosticsClient, EventLevel, Provider, SessionConfig};

/// `GCKeyword` of `Microsoft-Windows-DotNETRuntime`.
const GC_KEYWORD: u64 = 0x1;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut args = std::env::args().skip(1);
    let (Some(pid), Some(seconds), Some(out)) = (args.next(), args.next(), args.next()) else {
        eprintln!("usage: record_trace <pid> <seconds> <out-file>");
        std::process::exit(2);
    };
    let pid: u32 = pid.parse()?;
    let seconds: u64 = seconds.parse()?;

    let endpoint = wolf_ipc::discover()?
        .into_iter()
        .find(|e| e.pid == pid)
        .ok_or("no diagnostic endpoint for this pid")?;
    let client = Arc::new(DiagnosticsClient::new(endpoint));
    let config = SessionConfig::new(vec![Provider::new(
        "Microsoft-Windows-DotNETRuntime",
        GC_KEYWORD,
        EventLevel::Informational,
    )]);

    // The streaming thread: start the session, report its id, then copy chunks to the file
    // until the runtime closes the stream.
    let (started_tx, started_rx) = mpsc::channel();
    let reader = std::thread::spawn({
        let client = Arc::clone(&client);
        move || -> Result<u64, String> {
            let mut session = client.start_session(&config).map_err(|e| e.to_string())?;
            started_tx.send(session.id()).map_err(|e| e.to_string())?;
            let mut file = std::fs::File::create(&out).map_err(|e| e.to_string())?;
            let mut chunk = vec![0u8; 64 * 1024];
            let mut total = 0;
            loop {
                match session.read_chunk(&mut chunk).map_err(|e| e.to_string())? {
                    0 => return Ok(total),
                    n => {
                        file.write_all(&chunk[..n]).map_err(|e| e.to_string())?;
                        total += n as u64;
                    }
                }
            }
        }
    });

    let id = started_rx.recv().map_err(|_| "session did not start")?;
    tokio::time::sleep(Duration::from_secs(seconds)).await;
    client.stop_session(id).await?;
    // After the stop, the runtime flushes the rest and closes the stream; the thread ends.
    let bytes = reader.join().map_err(|_| "streaming thread panicked")??;
    println!("session {id}: wrote {bytes} bytes");
    Ok(())
}
