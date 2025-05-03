use std::{error::Error, net::SocketAddr, path::PathBuf};

use clap::Parser;
use cryo::protocol::StorageServer;

#[derive(Debug, Parser)]
struct Cli {
    /// Path to storage directory
    path: PathBuf,
    /// Listen for new connection at address
    address: SocketAddr,
}

fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();

    let cli = Cli::parse();
    let server = StorageServer::new(cli.address, cli.path)?;

    server.listen()?;
    Ok(())
}
