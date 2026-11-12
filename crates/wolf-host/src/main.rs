//! `mrwolf [--db PATH] <pid>...` — attach to running .NET processes and collect until
//! ctrl-c. Thin: argument parsing and the final report; the logic is in the library.

use std::path::PathBuf;
use std::process::ExitCode;

use wolf_host::{HostConfig, run};

fn main() -> ExitCode {
    let mut config = HostConfig::default();
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--db" => {
                let Some(path) = args.next() else {
                    return usage();
                };
                config.db_path = PathBuf::from(path);
            }
            "--help" | "-h" => {
                usage();
                return ExitCode::SUCCESS;
            }
            _ => match arg.parse::<u32>() {
                Ok(pid) => config.pids.push(pid),
                Err(_) => {
                    eprintln!("mrwolf: not a pid: {arg:?}");
                    return usage();
                }
            },
        }
    }
    if config.pids.is_empty() {
        return usage();
    }

    match run(&config) {
        Ok(report) => {
            println!(
                "mrwolf: done in {:?}: {} target(s) attached, {} detached; \
                 {} events ({} dropped, {} bytes); wrote {} points, {} events; \
                 {} sampler errors",
                report.uptime,
                report.attached,
                report.detached,
                report.readers.events,
                report.readers.dropped,
                report.readers.bytes,
                report.router.points_written,
                report.router.events_written,
                report.sample_errors,
            );
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("mrwolf: {error}");
            ExitCode::FAILURE
        }
    }
}

fn usage() -> ExitCode {
    eprintln!(
        "mrwolf {} — attach to running .NET processes and store their telemetry\n\
         usage: mrwolf [--db <path>] <pid>...\n\
         \n\
         --db <path>   store file (default: wolf.duckdb)\n\
         <pid>...      pids of .NET processes to watch; stop with ctrl-c",
        env!("CARGO_PKG_VERSION")
    );
    ExitCode::from(2)
}
