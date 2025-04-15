use log::trace;

use crate::storage::{
    error::PageErrorCause,
    header::row::{ROW_OFFSET_SIZE, ROW_RIGHT_OFFSET, ROW_VALUE, ROW_VALUE_SIZE},
};

use super::{
    error::{PageAction, StorageError},
    header::{
        page::{
            PAGE_CELLS, PAGE_HEADER_SIZE, PAGE_ID, PAGE_INTERNAL, PAGE_KIND, PAGE_LEAF,
            PAGE_PARENT, PAGE_SIZE,
        },
        row::ROW_BODY_SIZE,
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
            return matches!(kind, PageKind::Leaf { .. });
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
                action: PageAction::Select,
                cause: PageErrorCause::Unknown,
            })
        }
    }

    pub fn insert(&mut self, row: Row) -> Result<(), StorageError> {
        let bin_insert =
            |items: &mut Vec<Row>, row: Row| -> Result<(usize, Option<Row>), StorageError> {
                match items.binary_search(&row) {
                    Ok(_) => Err(StorageError::Page {
                        action: PageAction::Insert,
                        cause: PageErrorCause::Duplicate,
                    }),
                    Err(pos) => {
                        items.insert(pos, row);
                        Ok((pos, None))
                    }
                }
            };

        let size = row.size()?;
        self.cell_task(row, PageAction::Insert, bin_insert)?;
        self.cells += size;
        Ok(())
    }

    pub fn delete(&mut self, row: Row) -> Result<(), StorageError> {
        let delete =
            |items: &mut Vec<Row>, row: Row| -> Result<(usize, Option<Row>), StorageError> {
                match items.binary_search(&row) {
                    Ok(pos) => Ok((pos, Some(items.remove(pos)))),
                    Err(_) => Err(StorageError::Page {
                        action: PageAction::Delete,
                        cause: PageErrorCause::Missing,
                    }),
                }
            };

        let row = self.cell_task(row, PageAction::Delete, delete)?;
        if let Some(row) = row {
            self.cells -= row.size()?;
        }
        Ok(())
    }

    pub fn update(&mut self, row: Row) -> Result<Row, StorageError> {
        let bin_update =
            |items: &mut Vec<Row>, row: Row| -> Result<(usize, Option<Row>), StorageError> {
                match items.binary_search(&row) {
                    Ok(pos) => {
                        let out = items.remove(pos);
                        items.insert(pos, row);
                        Ok((pos, Some(out)))
                    }
                    Err(_) => Err(StorageError::Page {
                        action: PageAction::Update,
                        cause: PageErrorCause::Missing,
                    }),
                }
            };

        let replaced = row.size()? as isize;
        let actual =
            self.cell_task(row, PageAction::Update, bin_update)?
                .ok_or(StorageError::Page {
                    action: PageAction::Update,
                    cause: PageErrorCause::Missing,
                })?;
        let change: isize = replaced - actual.size()? as isize;
        self.cells = (self.cells as isize + change) as usize;

        Ok(actual)
    }

    pub fn retrieve(&mut self, row: Row) -> Result<Row, StorageError> {
        let retrieve =
            |items: &mut Vec<Row>, row: Row| -> Result<(usize, Option<Row>), StorageError> {
                match items.binary_search(&row) {
                    Ok(pos) => Ok((pos, Some(items[pos].clone()))),
                    Err(_) => Err(StorageError::Page {
                        action: PageAction::Retrieve,
                        cause: PageErrorCause::Missing,
                    }),
                }
            };

        self.cell_task(row, PageAction::Retrieve, retrieve)?
            .ok_or(StorageError::Page {
                action: PageAction::Update,
                cause: PageErrorCause::Missing,
            })
    }

    fn cell_task(
        &mut self,
        row: Row,
        action: PageAction,
        task: impl Fn(&mut Vec<Row>, Row) -> Result<(usize, Option<Row>), StorageError>,
    ) -> Result<Option<Row>, StorageError> {
        if let Some(mut kind) = self.kind.take() {
            let out = match &mut kind {
                PageKind::Internal { offsets } => {
                    if self.cells + row.size()? >= PAGE_SIZE {
                        self.kind = Some(kind);
                        return Err(StorageError::Page {
                            action,
                            cause: PageErrorCause::Full,
                        });
                    }

                    let (pos, row) = task(offsets, row)?;

                    // Update links
                    if pos < offsets.len() {
                        let offset = offsets[pos].left()?;
                        if pos > 0 {
                            offsets[pos - 1].set_right(offset);
                        }

                        let offset = offsets[pos].right()?;
                        if pos + 1 < offsets.len() {
                            offsets[pos + 1].set_left(offset);
                        }
                    }
                    row
                }
                PageKind::Leaf { rows } => {
                    if self.cells + row.size()? >= PAGE_SIZE {
                        self.kind = Some(kind);
                        return Err(StorageError::Page {
                            action,
                            cause: PageErrorCause::Full,
                        });
                    }

                    let (_, row) = task(rows, row)?;
                    row
                }
            };
            self.kind = Some(kind);
            Ok(out)
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
        trace!(
            "converting page to bytes {} cells: {} parent: {}",
            val.id, val.cells, val.parent
        );
        let mut buf = [0; PAGE_SIZE];

        buf[PAGE_ID..PAGE_CELLS].clone_from_slice(val.id.to_ne_bytes().as_ref());
        buf[PAGE_CELLS..PAGE_PARENT].clone_from_slice(val.cells.to_ne_bytes().as_ref());
        buf[PAGE_PARENT..PAGE_KIND].clone_from_slice(val.parent.to_ne_bytes().as_ref());

        let mut offset = PAGE_HEADER_SIZE;
        match val.kind.take() {
            Some(PageKind::Internal { offsets }) => {
                buf[PAGE_KIND] = PAGE_INTERNAL;
                offsets.iter().for_each(|cell| {
                    trace!(
                        "writing internal cell {} left: {} right: {}",
                        cell.id().unwrap(),
                        cell.left().unwrap(),
                        cell.right().unwrap()
                    );
                    let bytes: Vec<u8> = cell.into();
                    buf[offset..offset + bytes.len()].clone_from_slice(&bytes[..]);
                    offset += bytes.len();
                })
            }
            Some(PageKind::Leaf { rows }) => {
                buf[PAGE_KIND] = PAGE_LEAF;
                rows.iter().for_each(|cell| {
                    trace!(
                        "writing leaf cell {} {}",
                        cell.id().unwrap(),
                        cell.size().unwrap()
                    );
                    let bytes: Vec<u8> = cell.into();
                    buf[offset..offset + bytes.len()].clone_from_slice(&bytes[..]);
                    offset += bytes.len();
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
        trace!("reading page {id} from bytes");

        let parent =
            usize::from_ne_bytes(value[PAGE_PARENT..PAGE_KIND].try_into().map_err(|_| {
                StorageError::Page {
                    action: PageAction::Read,
                    cause: PageErrorCause::DataWrangling,
                }
            })?);
        let cells =
            usize::from_ne_bytes(value[PAGE_CELLS..PAGE_PARENT].try_into().map_err(|_| {
                StorageError::Page {
                    action: PageAction::Read,
                    cause: PageErrorCause::DataWrangling,
                }
            })?);
        let mut kind = match value[PAGE_KIND] {
            PAGE_LEAF => PageKind::Leaf {
                rows: Vec::with_capacity(cells),
            },
            PAGE_INTERNAL => PageKind::Internal {
                offsets: Vec::with_capacity(cells),
            },
            _ => {
                return Err(StorageError::Page {
                    action: PageAction::Read,
                    cause: PageErrorCause::DataWrangling,
                });
            }
        };

        let mut pos = PAGE_HEADER_SIZE;
        trace!("cells: {cells}, parent: {parent}");

        match &mut kind {
            PageKind::Internal { offsets } => {
                let mut offset = 0;
                while offset < cells {
                    let mut buf = [0; ROW_RIGHT_OFFSET + ROW_OFFSET_SIZE];
                    pos += offset;

                    buf[..].clone_from_slice(&value[pos..pos + ROW_RIGHT_OFFSET + ROW_OFFSET_SIZE]);
                    let row: Row = (&buf[..]).try_into().map_err(|_| StorageError::Page {
                        action: PageAction::Read,
                        cause: PageErrorCause::DataWrangling,
                    })?;

                    trace!(
                        "loading internal cell: {}, left: {}, right: {}",
                        row.id()?,
                        row.left()?,
                        row.right()?
                    );
                    offset += row.size()?;
                    offsets.push(row);
                }
            }
            PageKind::Leaf { rows } => {
                let mut offset = 0;
                while offset < cells {
                    let mut buf = [0; ROW_BODY_SIZE];
                    pos += offset;

                    buf[..].clone_from_slice(&value[pos..pos + ROW_BODY_SIZE]);
                    let mut row: Row = (&buf[..]).try_into().map_err(|_| StorageError::Page {
                        action: PageAction::Read,
                        cause: PageErrorCause::DataWrangling,
                    })?;

                    let len = usize::from_ne_bytes(
                        value[pos + ROW_VALUE..pos + ROW_VALUE + ROW_VALUE_SIZE]
                            .try_into()
                            .map_err(|_| StorageError::Page {
                                action: PageAction::Read,
                                cause: PageErrorCause::DataWrangling,
                            })?,
                    );
                    trace!(
                        "pos: {} offset: {} len: {} cells: {} row: {}",
                        pos,
                        offset,
                        len,
                        cells,
                        row.id()?
                    );
                    let content = &value[pos + ROW_BODY_SIZE..pos + ROW_BODY_SIZE + len];
                    row.set_value(content);

                    trace!("loading leaf cell: {}", row.id()?);
                    offset += row.size()?;
                    rows.push(row);
                }
            }
        }

        Ok(Page::new(offset, id, kind, cells, parent))
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::row::{Row, RowType, char_to_byte};

    use super::*;

    #[test]
    fn leaf_to_bytes() {
        let mut page = Page::new(0, 100, PageKind::Leaf { rows: vec![] }, 0, 0);
        let mut row = Row::new(RowType::Leaf);
        row.set_id(90);
        let expected_cells = row.size().unwrap();
        page.insert(row.clone()).unwrap();
        let bytes: [u8; PAGE_SIZE] = page.into();

        let mut page: Page = bytes.try_into().unwrap();
        assert_eq!(page.offset, 0);
        assert_eq!(page.id, 100);
        assert_eq!(page.cells, expected_cells);
        assert_eq!(page.parent, 0);
        assert!(matches!(page.kind, Some(PageKind::Leaf { .. })));
        assert_eq!(page.select().unwrap(), vec![row]);
    }

    #[test]
    fn internal_to_bytes() {
        let mut page = Page::new(0, 100, PageKind::Internal { offsets: vec![] }, 0, 0);
        let mut row = Row::new(RowType::Internal);
        row.set_id(90);
        let expected_cells = row.size().unwrap();
        page.insert(row.clone()).unwrap();
        let bytes: [u8; PAGE_SIZE] = page.into();

        let mut page: Page = bytes.try_into().unwrap();
        assert_eq!(page.offset, 0);
        assert_eq!(page.id, 100);
        assert_eq!(page.cells, expected_cells);
        assert_eq!(page.parent, 0);
        assert!(matches!(page.kind, Some(PageKind::Internal { .. })));
        assert_eq!(page.select().unwrap(), vec![row]);
    }

    #[test]
    fn leaf_insert_cell() {
        let mut page = Page::new(0, 0, PageKind::Leaf { rows: vec![] }, 0, 0);
        let mut row = Row::new(RowType::Leaf);
        row.set_id(90);
        let expected = row.size().unwrap();
        page.insert(row).unwrap();

        assert_eq!(page.cells, expected);
    }

    #[test]
    fn leaf_retrieve_cell() {
        let mut page = Page::new(0, 0, PageKind::Leaf { rows: vec![] }, 0, 0);
        let mut row = Row::new(RowType::Leaf);
        let expected = char_to_byte(vec!['t', 'e', 's', 't'].as_ref());
        row.set_id(90);
        row.set_value(&expected);
        page.insert(row).unwrap();

        row = Row::new(RowType::Leaf);
        row.set_id(90);
        let retrieved = page.retrieve(row).unwrap();

        assert_eq!(retrieved.value().unwrap(), expected);
    }

    #[test]
    fn leaf_update_cell() {
        let mut page = Page::new(0, 0, PageKind::Leaf { rows: vec![] }, 0, 0);
        let mut row = Row::new(RowType::Leaf);
        let initial = char_to_byte(vec!['t', 'e', 's', 't'].as_ref());
        row.set_id(90);
        row.set_value(&initial);
        page.insert(row).unwrap();

        row = Row::new(RowType::Leaf);
        let expected = char_to_byte(vec!['c', 'h', 'a', 'n', 'g', 'e', 'd'].as_ref());
        row.set_id(90);
        row.set_value(&expected);
        let size = row.size().unwrap();
        page.update(row).unwrap();

        row = Row::new(RowType::Leaf);
        row.set_id(90);
        let retrieved = page.retrieve(row).unwrap();

        assert_eq!(retrieved.value().unwrap(), expected);
        assert_eq!(page.cells, size);
    }

    #[test]
    fn leaf_delete_cell() {
        let mut page = Page::new(0, 0, PageKind::Leaf { rows: vec![] }, 0, 0);
        let mut row = Row::new(RowType::Leaf);
        let initial = char_to_byte(vec!['t', 'e', 's', 't'].as_ref());
        row.set_id(90);
        row.set_value(&initial);
        page.insert(row).unwrap();

        row = Row::new(RowType::Leaf);
        row.set_id(90);
        row.set_type(RowType::Leaf);
        page.delete(row).unwrap();

        row = Row::new(RowType::Leaf);
        row.set_id(90);
        row.set_type(RowType::Leaf);
        let retrieved = page.retrieve(row);
        if let Err(StorageError::Page {
            cause: PageErrorCause::Missing,
            ..
        }) = retrieved
        {
        } else {
            assert!(false)
        }
    }

    #[test]
    fn internal_insert_cell() {
        let mut page = Page::new(0, 0, PageKind::Internal { offsets: vec![] }, 0, 0);
        let mut row = Row::new(RowType::Internal);
        row.set_id(90);
        let expected = row.size().unwrap();
        page.insert(row).unwrap();

        assert_eq!(page.cells, expected);
    }

    #[test]
    fn leaf_select() {
        let mut page = Page::new(0, 0, PageKind::Leaf { rows: vec![] }, 0, 0);
        let mut row = Row::new(RowType::Leaf);
        row.set_id(90);
        page.insert(row.clone()).unwrap();

        assert_eq!(page.select().unwrap(), vec![row]);
    }

    #[test]
    fn internal_select() {
        let mut page = Page::new(0, 0, PageKind::Leaf { rows: vec![] }, 0, 0);
        let mut row = Row::new(RowType::Internal);
        row.set_id(90);
        page.insert(row.clone()).unwrap();

        assert_eq!(page.select().unwrap(), vec![row]);
    }
}
