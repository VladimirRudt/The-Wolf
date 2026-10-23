//! Lists running .NET processes: `cargo run -p wolf-ipc --example list_processes`.

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
    for endpoint in &endpoints {
        match wolf_ipc::process_info(endpoint).await {
            Ok(info) => {
                // Drop the build metadata: "10.0.0+abc123" -> "10.0.0".
                let runtime = info
                    .clr_product_version
                    .as_deref()
                    .map_or("?", |v| v.split('+').next().unwrap_or(v));
                let entrypoint = info.entrypoint_assembly.as_deref().unwrap_or("?");
                println!(
                    "{:>7}  {runtime:<10} {entrypoint:<20} {}",
                    endpoint.pid, info.command_line
                );
            }
            Err(e) => println!("{:>7}  error: {e}", endpoint.pid),
        }
    }
    Ok(())
}
