use std::{
    env::current_dir,
    error::Error,
    io::{self, Write},
};

/// Wrapper around STDIN that automatically flushes out to STDOUT
/// on every write.
struct StdOut<W: io::Write> {
    inner: W,
}

fn main() -> Result<(), Box<dyn Error>> {
    // Initialize env_logger; For logging to STDOUT/STDERR
    env_logger::init();

    let mut sreader;
    let mut swriter;
    let stdio = io::stdin();
    let stdout = io::stdout();
    let dir = current_dir()?;

    loop {
        sreader = stdio.lock();
        swriter = StdOut {
            inner: stdout.lock(),
        };

        swriter.write_all(format!("Hello World; path: {:?}", dir).as_bytes())?;
        break;
    }

    Ok(())
}

impl<W: io::Write> io::Write for StdOut<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let res = self.inner.write(buf);
        if res.is_ok() {
            self.inner.flush()?;
        }
        res
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}
