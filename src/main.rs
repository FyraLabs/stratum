use clap::Parser;
mod cli;
mod commit;
mod object;
mod store;
mod util;
mod state;

fn main() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .init();

    let cli = cli::Cli::parse();
    if let Err(e) = cli.run() {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}
