//! `mrwolf [--db DIR] [--shards N] <pid>...` — attach to running .NET processes and
//! collect until ctrl-c. `mrwolf watch [--db DIR] [--shards N] [pid]...` — the same, with
//! a live terminal view (P1-9); without pids it first shows a picker for targets and the
//! shard count. Thin: argument parsing and the final report; the logic is in the library.

use std::path::PathBuf;
use std::process::ExitCode;

use wolf_host::{HostConfig, run};

fn main() -> ExitCode {
    let mut config = HostConfig::default();
    let mut watch = false;
    let mut args = std::env::args().skip(1).peekable();
    if args.next_if_eq("watch").is_some() {
        watch = true;
    }
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--db" => {
                let Some(path) = args.next() else {
                    return usage();
                };
                config.db_dir = PathBuf::from(path);
            }
            "--shards" => {
                let Some(shards) = args.next().and_then(|s| s.parse::<u16>().ok()) else {
                    return usage();
                };
                config.shards = shards;
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
    if watch {
        config.watch = true;
        if config.pids.is_empty() {
            // No pids on the command line: pick targets (and shards) interactively.
            let Some(picked) = wolf_host::pick_targets(config.shards) else {
                return ExitCode::SUCCESS; // cancelled, or nothing to attach to
            };
            config.pids = picked.pids;
            config.shards = picked.shards;
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
                report.sources.events,
                report.sources.dropped,
                report.sources.bytes,
                report.shards.points_written,
                report.shards.events_written,
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
         usage: mrwolf [--db <dir>] [--shards <n>] <pid>...\n\
         \x20       mrwolf watch [--db <dir>] [--shards <n>] [pid]...\n\
         \n\
         watch         collect and show a live terminal view (charts, GC log, storage\n\
         \x20             queries); without pids, pick targets and shards interactively\n\
         --db <dir>    store directory with one shard-NN/ per shard (default: wolf.db)\n\
         --shards <n>  physical shards, one dotnet-gc + process → store block group each (default: 4)\n\
         <pid>...      pids of .NET processes to watch; stop with ctrl-c",
        env!("CARGO_PKG_VERSION")
    );
    ExitCode::from(2)
}
