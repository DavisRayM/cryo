use std::{
    error::Error,
    net::{SocketAddr, TcpListener, TcpStream},
    path::PathBuf,
    sync::{Arc, Mutex, mpsc},
    thread,
};

use log::{info, warn};

use crate::{
    Command,
    protocol::{ProtocolTransport, Request, Response, response::ResponseError},
    storage::{
        Row, StorageEngine,
        btree::BTree,
        log::{LogEntry, Logger},
        pager::Pager,
    },
};

use super::{ThreadPool, transport::TransportError};

pub struct StorageServer {
    address: SocketAddr,
    logger: Arc<Mutex<Logger>>,
    pool: ThreadPool,
}

const DATABASE_NAME: &str = "cryo.db";
const WAL_NAME: &str = "wal.log";

impl StorageServer {
    pub fn new(address: SocketAddr, dir: PathBuf) -> Result<Self, Box<dyn Error>> {
        let pager = Pager::open(dir.join(DATABASE_NAME))?;
        let logger = Logger::open(dir.join(WAL_NAME), pager)?;

        Ok(Self {
            logger: Arc::new(Mutex::new(logger)),
            address,
            pool: ThreadPool::new(15),
        })
    }

    pub fn listen(self) -> Result<(), TransportError> {
        info!("listening at {}", self.address);
        let listener = TcpListener::bind(self.address)?;
        let (sender, receiver) = mpsc::channel();
        let tx = sender.clone();

        ctrlc::set_handler(move || {
            let _ = tx.send(None);
        })
        .expect("failed to set Ctrl-C signal handler.");

        let tx = sender.clone();
        thread::spawn(move || {
            for stream in listener.incoming() {
                match stream {
                    Ok(stream) => {
                        let _ = tx.send(Some(stream));
                    }
                    Err(e) => {
                        info!("broken listener: {e:?}");
                        let _ = tx.send(None);
                        break;
                    }
                }
            }
        });

        while let Ok(event) = receiver.recv() {
            match event {
                Some(stream) => {
                    let logger = Arc::clone(&self.logger);
                    self.pool.execute(move || {
                        handle_connection(stream, logger).expect("connection failed")
                    });
                }
                None => {
                    info!("shutting down server.");
                    break;
                }
            }
        }
        Ok(())
    }
}

impl Drop for StorageServer {
    fn drop(&mut self) {
        let logger = Arc::clone(&self.logger);
        logger
            .lock()
            .unwrap()
            .log(LogEntry::GlobalCheckpoint)
            .unwrap();
    }
}

fn handle_connection(stream: TcpStream, logger: Arc<Mutex<Logger>>) -> Result<(), TransportError> {
    let mut transport = ProtocolTransport::new(stream);

    loop {
        let req = transport.read_request()?;
        info!("received request: {req:?}");

        let resp = match req {
            Request::CloseConnection => {
                transport.write_response(Response::ConnectionClosed)?;
                return Ok(());
            }
            Request::PrintStructure => {
                let mut logger = logger.lock().unwrap();
                let mut btree = BTree::new(logger.pager());

                match btree.structure() {
                    Ok(structure) => Response::Structure { out: structure },
                    Err(e) => Response::Err {
                        code: ResponseError::Command,
                        description: e.to_string(),
                    },
                }
            }
            Request::Populate(size) => {
                let mut logger = logger.lock().unwrap();
                let mut btree = BTree::new(logger.pager());

                match btree.execute(Command::Populate(size)) {
                    Ok(_) => Response::StateChanged,
                    Err(e) => Response::Err {
                        code: ResponseError::Command,
                        description: e.to_string(),
                    },
                }
            }
            Request::Ping => Response::Pong,
            Request::Query { kind, row } => {
                let res: Result<Row, _> = row.as_slice().try_into();
                let mut logger = logger.lock().unwrap();
                let mut btree = BTree::new(logger.pager());

                match res {
                    Ok(row) => match kind {
                        crate::protocol::request::QueryKind::Select => match btree.select() {
                            Ok(rows) => {
                                let rows =
                                    rows.iter().flat_map(|r| r.as_bytes()).collect::<Vec<u8>>();
                                Response::Query { rows }
                            }
                            Err(e) => Response::Err {
                                code: ResponseError::Query,
                                description: e.to_string(),
                            },
                        },
                        crate::protocol::request::QueryKind::Insert => {
                            match logger.log(LogEntry::Insert(row.as_bytes())) {
                                Ok(_) => Response::StateChanged,
                                Err(e) => Response::Err {
                                    code: ResponseError::Query,
                                    description: e.to_string(),
                                },
                            }
                        }
                        crate::protocol::request::QueryKind::Update => {
                            match logger.log(LogEntry::Update(row.as_bytes())) {
                                Ok(_) => Response::StateChanged,
                                Err(e) => Response::Err {
                                    code: ResponseError::Query,
                                    description: e.to_string(),
                                },
                            }
                        }
                        crate::protocol::request::QueryKind::Delete => {
                            match logger.log(LogEntry::Delete(row.as_bytes())) {
                                Ok(_) => Response::StateChanged,
                                Err(e) => Response::Err {
                                    code: ResponseError::Query,
                                    description: e.to_string(),
                                },
                            }
                        }
                    },
                    Err(e) => Response::Err {
                        code: ResponseError::Read,
                        description: e.to_string(),
                    },
                }
            }
        };

        transport.write_response(resp)?;
    }
}
