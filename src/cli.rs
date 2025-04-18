//! CLI utilities for Cryo.
//!
//! The utilities present in this module can be used to create a CLI tool for the Database.
use std::io::{BufRead, Write};

/// Possible commands from a user.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Command {
    /// Exit command `.exit`
    Exit,
    /// Request the storage to print out it's structure
    Structure,
    /// Populate,
    Populate(usize),
    /// DSL Statements
    Statement(String),
}

/// Prompt user for a valid Cryo command.
///
/// # Panics
/// If user inputted string is not a valid statement/command.
pub fn prompt<R, W>(mut reader: R, mut writer: W) -> Result<Command, String>
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
        ".exit" => Ok(Command::Exit),
        ".structure" => Ok(Command::Structure),
        s if s.starts_with(".populate") => {
            let parts = s.split(' ').collect::<Vec<&str>>();
            if parts.len() < 2 {
                return Err(
                    "populate command requires int argument for the number of records".into(),
                );
            }

            let records = parts[1]
                .parse::<usize>()
                .map_err(|_| "number of records should be a valid number".to_string())?;
            Ok(Command::Populate(records))
        }
        s if !s.starts_with(".") => Ok(Command::Statement(s.to_string())),
        s => Err(format!("unrecognized command '{}'", s)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prompt_prints_correctly() {
        let input = b".exit\n";
        let mut output = Vec::new();

        prompt(&input[..], &mut output).unwrap();

        let output = String::from_utf8(output).expect("not valid UTF-8");
        assert_eq!("> ", output);
    }

    #[test]
    fn prompt_handles_statements() {
        let input = b"\n";
        let mut output = Vec::new();

        let res = prompt(&input[..], &mut output).unwrap();
        assert_eq!(Command::Statement(String::default()), res);
    }

    #[test]
    #[should_panic(expected = "unrecognized command '.something_wrong'")]
    fn prompt_unrecognized_command() {
        let input = b".something_wrong\n";
        let mut output = Vec::new();

        prompt(&input[..], &mut output).unwrap();
    }
}
