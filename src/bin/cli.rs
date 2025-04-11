use std::io;

use cryo::cli::{Command, prompt};

fn main() {
    let mut reader;
    let mut writer;
    let stdio = io::stdin();
    let stdout = io::stdout();

    loop {
        reader = stdio.lock();
        writer = StdOut {
            inner: stdout.lock(),
        };

        match prompt(reader, writer) {
            Ok(Command::Exit) => break,
            Ok(Command::Statement(s)) => {
                println!("Statement: {}", s);
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
