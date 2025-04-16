use std::{
    error::Error,
    io::{self, BufRead, Write},
};

use cryo::Command;

fn main() -> Result<(), Box<dyn Error>> {
    // Initialize env_logger; For logging to STDOUT/STDERR
    env_logger::init();

    let mut stdio = io::stdin().lock();
    let mut stdout = io::stdout().lock();

    loop {
        let mut s = String::default();

        write!(&mut stdout, "> ")?;
        stdout.flush()?;

        stdio.read_line(&mut s)?;

        match <&str as TryInto<Command>>::try_into(s.as_str()) {
            Ok(cmd) => match cmd {
                Command::Exit => break,
                c => {
                    println!("command: {c:?}")
                }
            },
            Err(e) => eprintln!("error: {e}"),
        }
    }

    Ok(())
}
