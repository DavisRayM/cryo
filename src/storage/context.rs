/// Describes how a thread is currently accessing a cached page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccessMode {
    /// The page is being read.
    Read,
    /// The page is being mutated and will be marked dirty.
    Write,
}

/// Describes the context by the which the thread is accessing a cached page
#[derive(Debug, Clone, Copy)]
pub struct AccessContext {
    pub txn_id: Option<u64>,
    pub lsn: Option<u64>,
    pub reason: &'static str,
}

impl AccessContext {
    /// Access [`Page`] as part of a user-initiated transaction.
    pub const fn txn(
        txn_id: u64,
        lsn: Option<u64>,
        reason: &'static str,
    ) -> Self {
        Self {
            txn_id: Some(txn_id),
            lsn,
            reason: reason,
        }
    }

    /// Access [`Page`] as part of a maintenance process.
    pub const fn maintenance(reason: &'static str) -> Self {
        Self {
            txn_id: None,
            lsn: None,
            reason: reason,
        }
    }
}
