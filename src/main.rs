use std::sync::Arc;

use clap::Parser;
use cryo::{
    AccessContext,
    storage::{Tree, cursor::DebugOpt},
};
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

    let tree = Arc::new(Tree::load(cli.database, 10).unwrap());
    let mut ctx = AccessContext::maintenance("main test");
    let start = Arc::new(std::sync::Barrier::new(11));

    let mut handles = Vec::with_capacity(10);

    for i in 0..10 {
        let tree = Arc::clone(&tree);
        let start = Arc::clone(&start);

        handles.push(std::thread::spawn(move || {
            start.wait();

            let start = (i * 10) + 1;
            for k in start..start + 10 {
                tree.cursor()
                    .unwrap()
                    .insert(
                        &mut ctx,
                        &k,
                        "so much data"
                            .as_bytes()
                            .into(),
                    )
                    .unwrap();
            }
        }));
    }

    start.wait();
    std::thread::sleep(std::time::Duration::from_millis(100));
    for handle in handles {
        handle.join().unwrap();
    }

    tree.cursor()
        .unwrap()
        .debug_print(DebugOpt::default())
        .unwrap();
}
