use std::{env::current_dir, error::Error, io};

use cryo::*;

fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();

    let mut reader;
    let mut writer;
    let stdio = io::stdin();
    let stdout = io::stdout();
    let dir = current_dir().unwrap();
    let mut storage = BTreeStorage::new(dir).unwrap();

    loop {
        reader = stdio.lock();
        writer = StdOut {
            inner: stdout.lock(),
        };

        let cmd = match prompt(reader, writer) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("{}", e);
                continue;
            }
        };

        if let Command::Exit = cmd {
            if let Err(e) = storage.close() {
                eprintln!("failed to safely close database. error: {e}");
            }
            break;
        }

        match storage.query(cmd) {
            Ok(None) => {}
            Ok(Some(out)) => println!("{out}"),
            Err(e) => {
                eprintln!("query error: {e}")
            }
        }
    }

    Ok(())
}

/// StdOut wrapper than automatically flushes content after every write.
struct StdOut<W: io::Write> {
    inner: W,
}

impl<W: io::Write> io::Write for StdOut<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let res = self.inner.write(buf);
        if res.is_ok() {
            self.inner.flush()?
        }
        res
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}
