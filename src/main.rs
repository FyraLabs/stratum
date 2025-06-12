use clap::Parser;
mod cli;
mod commit;
mod mount;
mod object;
mod patchset;
mod state;
mod store;
mod util;
#[cfg(debug_assertions)]
const MAX_LEVEL: tracing::Level = tracing::Level::TRACE;
#[cfg(not(debug_assertions))]
const MAX_LEVEL: tracing::Level = tracing::Level::ERROR;

fn main() {
    tracing_subscriber::fmt()
        .with_max_level(MAX_LEVEL)
        .init();

    let cli = cli::Cli::parse();
    if let Err(e) = cli.run() {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}
