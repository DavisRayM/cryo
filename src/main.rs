use clap::Parser;

#[derive(Parser, Debug, Clone)]
struct Cli {
    database: String,
}

fn main() {
    let cli = Cli::parse();

    println!("{:#?}", cli);
    println!("Hello, world!");
}
