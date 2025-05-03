use std::{
    error::Error,
    net::{SocketAddr, TcpListener, TcpStream},
    path::PathBuf,
    sync::{Arc, Mutex},
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

        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    let handle = Arc::clone(&self.logger);
                    self.pool.execute(move || {
                        handle_connection(stream, handle, LogEntry::GlobalCheckpoint)
                            .expect("failed to handle connection")
                    });
                }
                Err(e) => warn!("broken connection: {e:?}"),
            }
        }
        Ok(())
    }
}

fn handle_connection(
    stream: TcpStream,
    logger: Arc<Mutex<Logger>>,
    checkpoint: LogEntry,
) -> Result<(), TransportError> {
    let mut transport = ProtocolTransport::new(stream);

    loop {
        let req = transport.read_request()?;
        info!("received request: {req:?}");

        let mut resp = match req {
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

        if resp == Response::StateChanged {
            let mut logger = logger.lock().unwrap();
            if let Err(err) = logger.log(checkpoint.clone()) {
                resp = Response::Err {
                    code: ResponseError::Query,
                    description: err.to_string(),
                }
            }
        }

        transport.write_response(resp)?;
    }
}
