use std::sync::Arc;

use clap::Parser;
use cryo::{AccessContext, Pager};
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

    let pager = Arc::new(Pager::open(cli.database, 10).unwrap());
    let start = Arc::new(std::sync::Barrier::new(11));

    let mut handles = Vec::with_capacity(10);

    for _ in 0..10 {
        let pager = Arc::clone(&pager);
        let start = Arc::clone(&start);

        handles.push(std::thread::spawn(move || {
            start.wait();

            pager
                .mut_page(1, AccessContext::maintenance("main test"), |_| {
                    std::thread::sleep(std::time::Duration::from_millis(250));
                })
                .unwrap();
        }));
    }

    start.wait();
    std::thread::sleep(std::time::Duration::from_millis(100));
    log::info!("Before thread join: {pager}");

    for handle in handles {
        handle.join().unwrap();
    }

    log::info!("After thread join: {pager}");
}
