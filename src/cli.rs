use std::io::{BufRead, Write};

/// Possible commands from a user.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Command {
    /// Exit command `.exit`
    Exit,
    Statement(String),
}

pub fn prompt<R, W>(mut reader: R, mut writer: W) -> Command
where
    R: BufRead,
    W: Write,
{
    let mut s = String::default();
    write!(&mut writer, "> ").expect("failed to write to writer.");

    reader
        .read_line(&mut s)
        .expect("failed to read from reader.");

    match s.trim_end() {
        ".exit" => Command::Exit,
        s if !s.starts_with(".") => Command::Statement(s.to_string()),
        _ => panic!("unrecognized command"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prompt_prints_correctly() {
        let input = b".exit\n";
        let mut output = Vec::new();

        prompt(&input[..], &mut output);

        let output = String::from_utf8(output).expect("not valid UTF-8");
        assert_eq!("> ", output);
    }

    #[test]
    fn prompt_handles_statements() {
        let input = b"\n";
        let mut output = Vec::new();

        let res = prompt(&input[..], &mut output);
        assert_eq!(Command::Statement(String::default()), res);
    }
}
