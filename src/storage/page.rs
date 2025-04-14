use crate::storage::{error::PageErrorCause, header::page::CELLS_PER_INTERNAL};

use super::{
    error::{PageAction, StorageError},
    header::{
        page::{
            CELLS_PER_LEAF, PAGE_CELLS, PAGE_HEADER_SIZE, PAGE_ID, PAGE_INTERNAL, PAGE_KIND,
            PAGE_LEAF, PAGE_PARENT, PAGE_SIZE,
        },
        row::{INTERNAL_ROW_SIZE, LEAF_ROW_SIZE},
    },
    row::Row,
};

#[derive(Debug, Clone)]
pub(crate) struct Page {
    pub kind: Option<PageKind>,
    pub id: usize,
    pub offset: usize,
    pub parent: usize,
    pub cells: usize,
}

#[derive(Debug, Clone)]
pub(crate) enum PageKind {
    Internal { offsets: Vec<Row> },
    Leaf { rows: Vec<Row> },
}

impl PartialEq for Page {
    fn eq(&self, other: &Self) -> bool {
        self.id.eq(&other.id) && self.same(other)
    }
}

impl Eq for Page {}

impl PartialOrd for Page {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Page {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.offset.cmp(&other.offset)
    }
}

impl Page {
    pub fn new(offset: usize, id: usize, kind: PageKind, cells: usize, parent: usize) -> Self {
        Self {
            cells,
            id,
            kind: Some(kind),
            offset,
            parent,
        }
    }

    pub fn leaf(&self) -> bool {
        if let Some(kind) = &self.kind {
            return match kind {
                PageKind::Leaf { .. } => true,
                _ => false,
            };
        }
        println!("{:?}", self);
        panic!("page unknown")
    }

    pub fn select(&mut self) -> Result<Vec<Row>, StorageError> {
        if let Some(kind) = self.kind.take() {
            match &kind {
                PageKind::Internal { offsets } => {
                    let out = offsets.clone();
                    self.kind = Some(kind);
                    Ok(out)
                }
                PageKind::Leaf { rows } => {
                    let out = rows.clone();
                    self.kind = Some(kind);
                    Ok(out)
                }
            }
        } else {
            Err(StorageError::Page {
                action: PageAction::Insert,
                cause: PageErrorCause::Unknown,
            })
        }
    }

    pub fn insert(&mut self, row: Row) -> Result<(), StorageError> {
        let bin_insert = |items: &mut Vec<Row>, row: Row| -> Result<(), StorageError> {
            match items.binary_search(&row) {
                Ok(_) => Err(StorageError::Page {
                    action: PageAction::Insert,
                    cause: PageErrorCause::Duplicate,
                }),
                Err(pos) => {
                    items.insert(pos, row);
                    Ok(())
                }
            }
        };

        if let Some(mut kind) = self.kind.take() {
            match &mut kind {
                PageKind::Internal { offsets } => {
                    if self.cells >= CELLS_PER_INTERNAL {
                        self.kind = Some(kind);
                        return Err(StorageError::Page {
                            action: PageAction::Insert,
                            cause: PageErrorCause::Full,
                        });
                    }

                    bin_insert(offsets, row)?;
                }
                PageKind::Leaf { rows } => {
                    if self.cells >= CELLS_PER_LEAF {
                        self.kind = Some(kind);
                        return Err(StorageError::Page {
                            action: PageAction::Insert,
                            cause: PageErrorCause::Full,
                        });
                    }

                    bin_insert(rows, row)?;
                }
            };
            self.kind = Some(kind);
            self.cells += 1;
            Ok(())
        } else {
            Err(StorageError::Page {
                action: PageAction::Insert,
                cause: PageErrorCause::Unknown,
            })
        }
    }

    fn same(&self, other: &Page) -> bool {
        match self.kind {
            Some(PageKind::Internal { .. }) => matches!(
                other.kind.as_ref().expect("comparing with unknown"),
                PageKind::Internal { .. }
            ),
            Some(PageKind::Leaf { .. }) => matches!(
                other.kind.as_ref().expect("comparing with unknown"),
                PageKind::Leaf { .. }
            ),
            _ => panic!("comparing with unknown"),
        }
    }
}

impl From<Page> for [u8; PAGE_SIZE] {
    fn from(mut val: Page) -> [u8; PAGE_SIZE] {
        let mut buf = [0; PAGE_SIZE];

        buf[PAGE_ID..PAGE_CELLS].clone_from_slice(val.id.to_ne_bytes().as_ref());
        buf[PAGE_CELLS..PAGE_PARENT].clone_from_slice(val.cells.to_ne_bytes().as_ref());
        buf[PAGE_PARENT..PAGE_KIND].clone_from_slice(val.parent.to_ne_bytes().as_ref());

        let mut offset = PAGE_HEADER_SIZE;
        match val.kind.take() {
            Some(PageKind::Internal { offsets }) => {
                buf[PAGE_KIND] = PAGE_INTERNAL;
                offsets.iter().for_each(|cell| {
                    let bytes: [u8; INTERNAL_ROW_SIZE] = cell.into();
                    buf[offset..offset + INTERNAL_ROW_SIZE].clone_from_slice(&bytes[..]);
                    offset += INTERNAL_ROW_SIZE;
                })
            }
            Some(PageKind::Leaf { rows }) => {
                buf[PAGE_KIND] = PAGE_LEAF;
                rows.iter().for_each(|cell| {
                    let bytes: [u8; LEAF_ROW_SIZE] = cell.into();
                    buf[offset..offset + LEAF_ROW_SIZE].clone_from_slice(&bytes[..]);
                    offset += LEAF_ROW_SIZE;
                })
            }
            None => {
                panic!("unknown page: has no kind.")
            }
        }

        buf
    }
}

impl TryFrom<[u8; PAGE_SIZE]> for Page {
    type Error = StorageError;

    fn try_from(value: [u8; PAGE_SIZE]) -> Result<Self, Self::Error> {
        let offset: usize = 0;
        let id = usize::from_ne_bytes(value[PAGE_ID..PAGE_CELLS].try_into().map_err(|_| {
            StorageError::Page {
                action: PageAction::Read,
                cause: PageErrorCause::DataWrangling,
            }
        })?);
        let mut kind = match value[PAGE_KIND] {
            PAGE_LEAF => PageKind::Leaf { rows: Vec::new() },
            PAGE_INTERNAL => PageKind::Internal {
                offsets: Vec::new(),
            },
            _ => {
                return Err(StorageError::Page {
                    action: PageAction::Read,
                    cause: PageErrorCause::DataWrangling,
                });
            }
        };
        let cells =
            usize::from_ne_bytes(value[PAGE_CELLS..PAGE_PARENT].try_into().map_err(|_| {
                StorageError::Page {
                    action: PageAction::Read,
                    cause: PageErrorCause::DataWrangling,
                }
            })?);
        let parent =
            usize::from_ne_bytes(value[PAGE_PARENT..PAGE_KIND].try_into().map_err(|_| {
                StorageError::Page {
                    action: PageAction::Read,
                    cause: PageErrorCause::DataWrangling,
                }
            })?);

        let mut pos = PAGE_HEADER_SIZE;

        match &mut kind {
            PageKind::Internal { offsets } => {
                for _ in 0..cells {
                    let mut buf = [0; INTERNAL_ROW_SIZE];

                    buf[..].clone_from_slice(&value[pos..pos + INTERNAL_ROW_SIZE]);
                    let row: Row = (&buf[..]).try_into().map_err(|_| StorageError::Page {
                        action: PageAction::Read,
                        cause: PageErrorCause::DataWrangling,
                    })?;
                    offsets.push(row);
                    pos += INTERNAL_ROW_SIZE;
                }
            }
            PageKind::Leaf { rows } => {
                for _ in 0..cells {
                    let mut buf = [0; LEAF_ROW_SIZE];

                    buf[..].clone_from_slice(&value[pos..pos + LEAF_ROW_SIZE]);
                    let row: Row = (&buf[..]).try_into().map_err(|_| StorageError::Page {
                        action: PageAction::Read,
                        cause: PageErrorCause::DataWrangling,
                    })?;
                    rows.push(row);
                    pos += INTERNAL_ROW_SIZE;
                }
            }
        }

        Ok(Page::new(offset, id, kind, cells, parent))
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::row::Row;

    use super::*;

    #[test]
    fn leaf_to_bytes() {
        let mut page = Page::new(0, 100, PageKind::Leaf { rows: vec![] }, 0, 0);
        let mut row = Row::new();
        row.set_id(90);
        page.insert(row.clone()).unwrap();
        let bytes: [u8; PAGE_SIZE] = page.into();

        let mut page: Page = bytes.try_into().unwrap();
        assert_eq!(page.offset, 0);
        assert_eq!(page.id, 100);
        assert_eq!(page.cells, 1);
        assert_eq!(page.parent, 0);
        assert!(matches!(page.kind, Some(PageKind::Leaf { .. })));
        assert_eq!(page.select().unwrap(), vec![row]);
    }

    #[test]
    fn internal_to_bytes() {
        let mut page = Page::new(0, 100, PageKind::Internal { offsets: vec![] }, 0, 0);
        let mut row = Row::new();
        row.set_id(90);
        page.insert(row.clone()).unwrap();
        let bytes: [u8; PAGE_SIZE] = page.into();

        let mut page: Page = bytes.try_into().unwrap();
        assert_eq!(page.offset, 0);
        assert_eq!(page.id, 100);
        assert_eq!(page.cells, 1);
        assert_eq!(page.parent, 0);
        assert!(matches!(page.kind, Some(PageKind::Internal { .. })));
        assert_eq!(page.select().unwrap(), vec![row]);
    }

    #[test]
    fn leaf_insert_cell() {
        let mut page = Page::new(0, 0, PageKind::Leaf { rows: vec![] }, 0, 0);
        let mut row = Row::new();
        row.set_id(90);
        page.insert(row).unwrap();

        assert_eq!(page.cells, 1);
    }

    #[test]
    fn internal_insert_cell() {
        let mut page = Page::new(0, 0, PageKind::Internal { offsets: vec![] }, 0, 0);
        let mut row = Row::new();
        row.set_id(90);
        page.insert(row).unwrap();

        assert_eq!(page.cells, 1);
    }

    #[test]
    fn leaf_select() {
        let mut page = Page::new(0, 0, PageKind::Leaf { rows: vec![] }, 0, 0);
        let mut row = Row::new();
        row.set_id(90);
        page.insert(row.clone()).unwrap();

        assert_eq!(page.select().unwrap(), vec![row]);
    }

    #[test]
    fn internal() {
        let mut page = Page::new(0, 0, PageKind::Leaf { rows: vec![] }, 0, 0);
        let mut row = Row::new();
        row.set_id(90);
        page.insert(row.clone()).unwrap();

        assert_eq!(page.select().unwrap(), vec![row]);
    }
}
