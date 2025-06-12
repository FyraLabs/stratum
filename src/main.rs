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
const MAX_LEVEL: tracing::Level = tracing::Level::TRACE;

fn main() {
    // Configure tracing with specific crate filtering
    use tracing_subscriber::{filter::EnvFilter, fmt, prelude::*};

    // Create a filter that allows our max level but silences sled
    let filter = EnvFilter::builder()
        .with_default_directive(MAX_LEVEL.into())
        .from_env_lossy()
        .add_directive("sled=off".parse().unwrap());

    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(filter)
        .init();

    let cli = cli::Cli::parse();
    if let Err(e) = cli.run() {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}
