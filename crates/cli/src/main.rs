//! `shakedown` command-line binary (stub).
//!
//! What: prints usage, version, and placeholder subcommand names until the runner is implemented.
//!
//! Why: reserves the crate/binary name and gives users a single entry point to discover.
//!
//! How: run `shakedown`, `shakedown --help`, or `shakedown --version`.

use std::process;

const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Parse argv and dispatch help, version, or a placeholder error for unknown commands.
fn main() {
    let args: Vec<String> = std::env::args().collect();

    if args.len() == 1 {
        print_usage();
        return;
    }

    match args[1].as_str() {
        "--version" | "-V" => {
            println!("shakedown {}", VERSION);
        }
        "--help" | "-h" => {
            print_usage();
        }
        _ => {
            eprintln!("Unknown command: {}", args[1]);
            print_usage();
            process::exit(1);
        }
    }
}

/// Write usage text to stdout (commands mirror the future product surface).
fn print_usage() {
    println!("shakedown {}", VERSION);
    println!();
    println!("Shakedown testing framework for Rust services");
    println!();
    println!("USAGE:");
    println!("    shakedown [COMMAND] [OPTIONS]");
    println!();
    println!("COMMANDS:");
    println!("    run   Run a scenario");
    println!("    list  List available scenarios");
    println!();
    println!("OPTIONS:");
    println!("    -h, --help       Prints help information");
    println!("    -V, --version    Prints version information");
}
