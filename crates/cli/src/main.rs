/// Shakedown CLI - Shakedown testing framework for Rust services
///
/// Usage: `shakedown [COMMAND]`
///
/// Commands:
/// - run   Run a scenario
/// - list  List available scenarios
///
/// For more information, see the project documentation.
use std::process;

const VERSION: &str = env!("CARGO_PKG_VERSION");

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
