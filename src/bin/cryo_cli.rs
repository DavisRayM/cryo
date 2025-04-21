use clap::Parser;
use std::{
    error::Error,
    io::{self, BufRead, Write},
    path::PathBuf,
};

use cryo::{
    Command,
    storage::{StorageEngine, btree::BTree, pager::Pager},
};

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    /// Path to storage directory
    path: PathBuf,
}

fn main() -> Result<(), Box<dyn Error>> {
    // Initialize env_logger; For logging to STDOUT/STDERR
    env_logger::init();

    let cli = Cli::parse();
    let mut stdio = io::stdin().lock();
    let mut stdout = io::stdout().lock();

    if !cli.path.is_dir() {
        panic!("'{:?}' is not a directory", cli.path);
    }
    let store = cli.path.join("cryo.db");
    let mut pager = Pager::open(store)?;
    let mut btree = BTree::new(&mut pager);

    loop {
        let mut s = String::default();

        write!(&mut stdout, "> ")?;
        stdout.flush()?;

        stdio.read_line(&mut s)?;

        match <&str as TryInto<Command>>::try_into(s.as_str()) {
            Ok(cmd) => match cmd {
                c if c == Command::Exit => {
                    btree.execute(c)?;
                    break;
                }
                c => {
                    btree.execute(c)?;
                }
            },
            Err(e) => eprintln!("error: {e}"),
        }
    }

    Ok(())
}
