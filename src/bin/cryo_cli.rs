use clap::Parser;
use std::{
    error::Error,
    fs::OpenOptions,
    io::{self, BufRead, Write},
    net::{SocketAddr, TcpStream},
    path::PathBuf,
    process::exit,
    str::FromStr,
};

use cryo::{
    Command,
    protocol::{ProtocolTransport, Response},
    statement::print_row,
    storage::Row,
};

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    /// Address of the Cryo server.
    address: Option<SocketAddr>,
}

fn main() -> Result<(), Box<dyn Error>> {
    // Initialize env_logger; For logging to STDOUT/STDERR
    env_logger::init();

    let mut stdio = io::stdin().lock();
    let mut stdout = io::stdout().lock();
    let cli = Cli::parse();

    let address = cli
        .address
        .unwrap_or(SocketAddr::from_str("127.0.0.1:8000")?);
    let stream = TcpStream::connect(address)?;
    let mut transport = ProtocolTransport::new(stream);

    match transport.write_command(Command::Ping) {
        Ok(_) => {
            if transport.read_response()? != Response::Pong {
                eprintln!("unexpected response from server.");
                exit(1)
            }
        }
        _ => {
            eprintln!("failed to establish connection.");
            exit(1)
        }
    }

    loop {
        let mut s = String::default();

        write!(&mut stdout, "> ")?;
        stdout.flush()?;

        stdio.read_line(&mut s)?;

        match <&str as TryInto<Command>>::try_into(s.as_str()) {
            Ok(cmd) => {
                let mut path: Option<PathBuf> = None;
                if let Command::Structure(out) = cmd.clone() {
                    path = out;
                }
                if let Err(_) = transport.write_command(cmd) {
                    eprintln!("broken connection");
                    break Ok(());
                }

                match transport.read_response()? {
                    Response::Ok | Response::StateChanged => {}
                    Response::Pong => println!("PONG"),
                    Response::Query { mut rows } => {
                        while !rows.is_empty() {
                            let row: Row = rows.as_slice().try_into()?;
                            rows.drain(0..row.as_bytes().len());

                            println!("{}", print_row(&row))
                        }
                    }
                    Response::Structure { out } => {
                        if let Some(path) = path {
                            let mut f = OpenOptions::new()
                                .create(true)
                                .truncate(true)
                                .write(true)
                                .open(path)?;
                            f.write_all(out.as_bytes())?;
                        } else {
                            println!("Structure:\n{out}");
                        }
                    }
                    Response::Err { code, description } => {
                        eprintln!("error({code:?}): {description}")
                    }
                    Response::ConnectionClosed => {
                        println!("connection closed");
                        return Ok(());
                    }
                }
            }
            Err(e) => eprintln!("error: {e}"),
        }
    }
}
