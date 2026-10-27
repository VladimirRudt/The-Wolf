//! Lists running .NET processes: `cargo run -p wolf-ipc --example list_processes`.

use wolf_ipc::DiagnosticsClient;

#[tokio::main(flavor = "current_thread")]
async fn main() -> std::io::Result<()> {
    let endpoints = wolf_ipc::discover()?;
    if endpoints.is_empty() {
        println!("No .NET processes found.");
        return Ok(());
    }

    println!(
        "{:>7}  {:<10} {:<20} COMMAND LINE",
        "PID", "RUNTIME", "ENTRYPOINT"
    );
    for endpoint in endpoints {
        let pid = endpoint.pid;
        let client = DiagnosticsClient::new(endpoint);
        match client.process_info().await {
            Ok(info) => {
                // Drop the build metadata: "10.0.0+abc123" -> "10.0.0".
                let runtime = info
                    .clr_product_version
                    .as_deref()
                    .map_or("?", |v| v.split('+').next().unwrap_or(v));
                let entrypoint = info.entrypoint_assembly.as_deref().unwrap_or("?");
                println!(
                    "{pid:>7}  {runtime:<10} {entrypoint:<20} {}",
                    info.command_line
                );
            }
            Err(e) => println!("{pid:>7}  error: {e}"),
        }
    }
    Ok(())
}
