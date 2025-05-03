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
    storage::{Row, StorageEngine, btree::BTree, pager::Pager},
};

use super::{thread::ThreadPool, transport::TransportError};

#[derive(Debug)]
pub struct StorageServer {
    address: SocketAddr,
    pager: Arc<Mutex<Pager>>,
    pool: ThreadPool,
}

impl StorageServer {
    pub fn new(address: SocketAddr, path: PathBuf) -> Result<Self, Box<dyn Error>> {
        Ok(Self {
            pager: Arc::new(Mutex::new(Pager::open(path)?)),
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
                    let handle = Arc::clone(&self.pager);
                    self.pool.execute(move || {
                        handle_connection(stream, handle).expect("failed to handle connection")
                    });
                }
                Err(e) => warn!("broken connection: {e:?}"),
            }
        }
        Ok(())
    }
}

fn handle_connection(stream: TcpStream, pager: Arc<Mutex<Pager>>) -> Result<(), TransportError> {
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
                let mut pager = pager.lock().unwrap();
                let mut btree = BTree::new(&mut pager);

                match btree.structure() {
                    Ok(structure) => Response::Structure { out: structure },
                    Err(e) => Response::Err {
                        code: ResponseError::Command,
                        description: e.to_string(),
                    },
                }
            }
            Request::Populate(size) => {
                let mut pager = pager.lock().unwrap();
                let mut btree = BTree::new(&mut pager);

                match btree.execute(Command::Populate(size)) {
                    Ok(_) => Response::Ok,
                    Err(e) => Response::Err {
                        code: ResponseError::Command,
                        description: e.to_string(),
                    },
                }
            }
            Request::Ping => Response::Pong,
            Request::Query { kind, row } => {
                let res: Result<Row, _> = row.as_slice().try_into();
                let mut pager = pager.lock().unwrap();
                let mut btree = BTree::new(&mut pager);

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
                        crate::protocol::request::QueryKind::Insert => match btree.insert(row) {
                            Ok(_) => Response::Ok,
                            Err(e) => Response::Err {
                                code: ResponseError::Query,
                                description: e.to_string(),
                            },
                        },
                        crate::protocol::request::QueryKind::Delete => match btree.delete(row) {
                            Ok(_) => Response::Ok,
                            Err(e) => Response::Err {
                                code: ResponseError::Query,
                                description: e.to_string(),
                            },
                        },
                        crate::protocol::request::QueryKind::Update => match btree.update(row) {
                            Ok(_) => Response::Ok,
                            Err(e) => Response::Err {
                                code: ResponseError::Query,
                                description: e.to_string(),
                            },
                        },
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
