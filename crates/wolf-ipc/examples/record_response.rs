//! Records a raw ProcessInfo3 response for golden-file tests:
//! `cargo run -p wolf-ipc --example record_response -- <pid> <out-file>`.

use std::error::Error;

use wolf_ipc::CommandCode;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut args = std::env::args().skip(1);
    let (Some(pid), Some(out)) = (args.next(), args.next()) else {
        eprintln!("usage: record_response <pid> <out-file>");
        std::process::exit(2);
    };
    let pid: u32 = pid.parse()?;

    let endpoint = wolf_ipc::discover()?
        .into_iter()
        .find(|e| e.pid == pid)
        .ok_or("no diagnostic endpoint for this pid")?;
    let message = wolf_ipc::request(&endpoint, CommandCode::PROCESS_INFO3, &[]).await?;
    let bytes = message.to_bytes()?;
    std::fs::write(&out, &bytes)?;
    println!("wrote {} bytes to {out}", bytes.len());
    Ok(())
}
