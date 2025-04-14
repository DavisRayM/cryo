use std::{env::current_dir, io};

use cryo::*;

fn main() {
    env_logger::init();

    let mut reader;
    let mut writer;
    let stdio = io::stdin();
    let stdout = io::stdout();
    let dir = current_dir().unwrap();
    println!("storage created!");
    let mut storage = BTreeStorage::new(dir).unwrap();

    loop {
        reader = stdio.lock();
        writer = StdOut {
            inner: stdout.lock(),
        };

        match prompt(reader, writer) {
            Ok(Command::Exit) => {
                storage.query(Command::Exit).unwrap();
                break;
            }
            Ok(c) => {
                let out = storage.query(c).unwrap();
                if let Some(out) = out {
                    println!("{out}");
                }
            }
            Err(e) => eprintln!("{}", e),
        }
    }
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
