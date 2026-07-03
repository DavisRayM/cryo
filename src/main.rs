use clap::Parser;
use cryo::pager::Pager;
use env_logger::Env;

#[derive(Parser, Debug, Clone)]
struct Cli {
    database: String,
}

fn main() {
    env_logger::init_from_env(
        Env::default().filter_or("CRYO_LOG_LEVEL", "DEBUG"),
    );
    let cli = Cli::parse();

    let _pager = Pager::open(cli.database, 10).unwrap();
}
