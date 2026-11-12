//! Link fix for the bundled DuckDB build on Windows MSVC: libduckdb-sys compiles
//! DuckDB's file-lock diagnostics, which call the Restart Manager API
//! (`RmStartSession`/`RmGetList`/...), but does not emit the corresponding
//! `rustc-link-lib` directive, so final link fails with unresolved externals.
//! Add `rstrtmgr` here; it ships with the Windows SDK that the toolchain already uses.

fn main() {
    let target_os = std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    let target_env = std::env::var("CARGO_CFG_TARGET_ENV").unwrap_or_default();
    if target_os == "windows" && target_env == "msvc" {
        println!("cargo:rustc-link-lib=dylib=rstrtmgr");
    }
}
