use std::{io, sync::Arc};

use log::trace;

use crate::{
    AccessContext, KEYCELL_SIZE, Key, KeyCell, Page, PageFlags, ValueCell,
    btree::TreeInner, storage::constants::page::HEADER_SIZE,
};

pub struct Cursor {
    breadcrumbs: Vec<(usize, KeyCell)>,
    current_page: usize,
    tree: Arc<TreeInner>,
}

#[derive(Debug, Clone)]
struct CursorValue {
    pub idx: usize,
    pub val_cell: ValueCell,
}

impl From<(usize, ValueCell)> for CursorValue {
    fn from(value: (usize, ValueCell)) -> Self {
        Self {
            idx: value.0,
            val_cell: value.1,
        }
    }
}

impl Cursor {
    /// Initialize a [`Cursor`] at `root` position of [`super::Tree`]
    pub fn from_root(tree: &Arc<TreeInner>) -> io::Result<Self> {
        Ok(Self {
            breadcrumbs: vec![],
            current_page: tree.root()?,
            tree: tree.clone(),
        })
    }

    /// Inserts a new `key` & `value` into the tree.
    ///
    /// TODO: Decide on self-split or user-initiated...
    pub fn insert(
        &mut self,
        ctx: AccessContext,
        key: &Key,
        value: Vec<u8>,
    ) -> io::Result<Option<ValueCell>> {
        while !self.find(ctx, key)? {
            trace!(
                "tree cursor search: key={key} current_page={} depth={}",
                self.current_page,
                self.breadcrumbs.len()
            );
        }

        let new_cell = ValueCell {
            key: *key,
            value: value.into(),
        };

        match self.read_value(ctx, key)? {
            Some(old_cell) => {
                self.insert_existing(ctx, &old_cell, &new_cell)?;
                Ok(Some(old_cell.val_cell))
            }
            None => {
                self.insert_new(ctx, &new_cell)?;
                Ok(None)
            }
        }
    }

    /// Locate the `Leaf` page that would hold `key` and return `key`
    /// bytes if present. This action will leave the cursor at the leaf node.
    pub fn search(&mut self, key: &Key) -> io::Result<Option<ValueCell>> {
        while !self.find(
            AccessContext::maintenance("tree cursor search locate leaf node"),
            key,
        )? {
            trace!(
                "tree cursor search: key={key} current_page={} depth={}",
                self.current_page,
                self.breadcrumbs.len()
            );
        }

        let out = self.read_value(
            AccessContext::maintenance("tree cursor search read value"),
            key,
        )?;
        trace!(
            "tree cursor search complete: page={} key={} found={} depth={}",
            self.current_page,
            key,
            out.is_some(),
            self.breadcrumbs.len()
        );

        Ok(out.map(|cv| cv.val_cell))
    }
}

impl Cursor {
    /// Inserts new value cell in the page cursor is currently in
    fn insert_new(
        &self,
        ctx: AccessContext,
        cell: &ValueCell,
    ) -> io::Result<()> {
        self.tree
            .pager
            .mut_page(self.current_page, ctx, |page| {
                let key_count = page.num_keys() as usize;
                let keys = self.key_range(0, key_count, page)?;
                let new_count = key_count + 1;

                let free_space = page.free_space() as usize;
                let free_space_start = page.free_space_start() as usize;
                let free_space_end = page.free_space_end() as usize;

                if cell.len() > free_space {
                    return Err(io::Error::new(
                        io::ErrorKind::StorageFull,
                        "cursor insert to page would overflow",
                    ));
                }

                let new_space_end = free_space_end - cell.len();
                let new_space_start = HEADER_SIZE + (new_count * KEYCELL_SIZE);
                let new_space = free_space - cell.len();

                if new_space_end < free_space_start {
                    return Err(io::Error::other("page is heavily fragmented"));
                }

                let key = KeyCell {
                    key: cell.key,
                    offset: new_space_end as u32,
                };

                match keys.binary_search(&key) {
                    Err(pos) => {
                        let key_offset = HEADER_SIZE + (pos * KEYCELL_SIZE);
                        let (_left, right) = keys.split_at(pos);
                        let mut key_bytes =
                            Into::<[u8; KEYCELL_SIZE]>::into(&key).to_vec();
                        let after_insert = right
                            .iter()
                            .flat_map(Into::<[u8; KEYCELL_SIZE]>::into)
                            .collect::<Vec<_>>();
                        key_bytes.extend(after_insert);

                        let value_bytes: Box<[u8]> = cell.into();

                        page.mut_cell(new_space_end, free_space_end)
                            .copy_from_slice(&value_bytes);
                        page.mut_cell(key_offset, new_space_start)
                            .copy_from_slice(&key_bytes);

                        page.set_free_space_end(new_space_end as u16);
                        page.set_free_space_start(new_space_start as u16);
                        page.set_free_space(new_space as u16);
                        page.set_num_keys(new_count as u16);

                        Ok(())
                    }
                    Ok(_) => Err(io::Error::other(
                        "called insert new on existing key",
                    )),
                }
            })?
    }

    /// Update `old` in the page [`Cursor`] is in with `updated`.
    fn insert_existing(
        &self,
        ctx: AccessContext,
        old: &CursorValue,
        updated: &ValueCell,
    ) -> io::Result<()> {
        self.tree
            .pager
            .mut_page(self.current_page, ctx, |page| {
                let mut key = self
                    .key_range(old.idx, old.idx + 1, page)?
                    .pop()
                    .expect("should have old key");

                let free_space = page.free_space() as usize;
                let free_space_start = page.free_space_start() as usize;
                let free_space_end = page.free_space_end() as usize;

                let mut new_space = free_space + old.val_cell.len();
                let new_space_end = free_space_end - updated.len();

                if updated.len() > new_space {
                    return Err(io::Error::new(
                        io::ErrorKind::StorageFull,
                        "cursor insert to page would overflow",
                    ));
                }

                if new_space_end < free_space_start {
                    return Err(io::Error::other("page is heavily fragmented"));
                }

                key.offset = new_space_end as u32;
                let key_bytes: [u8; KEYCELL_SIZE] = (&key).into();
                let value_bytes: Box<[u8]> = updated.into();
                let key_offset = HEADER_SIZE + (old.idx * KEYCELL_SIZE);
                new_space -= value_bytes.len();

                page.mut_cell(new_space_end, free_space_end)
                    .copy_from_slice(&value_bytes);
                page.mut_cell(key_offset, key_offset + KEYCELL_SIZE)
                    .copy_from_slice(&key_bytes);

                page.set_free_space_end(new_space_end as u16);
                page.set_free_space(new_space as u16);

                Ok(())
            })?
    }

    /// Attempts to read [`CursorValue`] of associated `key` in page.
    ///
    /// ## Errors
    ///
    /// This funtion will error if a read attempt is made on an internal
    /// [`Page`]. Utilize `Self::key_range` instead.
    fn read_value(
        &self,
        ctx: AccessContext,
        key: &Key,
    ) -> io::Result<Option<CursorValue>> {
        self.tree
            .pager
            .page(self.current_page, ctx, |page| {
                let Some(flags) = PageFlags::from_bits(page.flags()) else {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("corrupted page {} flags", self.current_page),
                    ));
                };

                if !flags.contains(PageFlags::IsLeaf) {
                    return Err(io::Error::new(
                        io::ErrorKind::Unsupported,
                        format!(
                            "attempt to read non-leaf page {}",
                            self.current_page
                        ),
                    ));
                }

                let count = page.num_keys() as usize;
                let keys = self.key_range(0, count, page)?;

                match keys.binary_search(&KeyCell::with_key(key)) {
                    Ok(pos) => {
                        let key = keys[pos].clone();
                        let value = ValueCell::from_bytes(
                            page.cell_from(key.offset as usize),
                        )?;

                        Ok(Some((pos, value).into()))
                    }
                    Err(_) => Ok(None),
                }
            })?
    }

    /// Locates the next hop to `key` from `Self::current_page`, updating
    /// current page and pushing breadcrumb.
    ///
    /// This function will return `true` once a `Leaf` page is located.
    fn find(&mut self, ctx: AccessContext, key: &Key) -> io::Result<bool> {
        let next_location =
            self.tree
                .pager
                .page(self.current_page, ctx, |page| {
                    let Some(flags) = PageFlags::from_bits(page.flags()) else {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!(
                                "corrupted page {} flags",
                                self.current_page
                            ),
                        ));
                    };

                    if flags.contains(PageFlags::IsLeaf) {
                        return Ok(None);
                    }

                    let count = page.num_keys() as usize;
                    let keys = self.key_range(0, count, page)?;

                    let next_page =
                        match keys.binary_search(&KeyCell::with_key(key)) {
                            Ok(pos) => keys[pos].clone(),
                            Err(pos) => {
                                if pos >= keys.len() {
                                    KeyCell {
                                        key: page.high_key() as u32,
                                        offset: page.right_pointer(),
                                    }
                                } else {
                                    keys[pos - 1].clone()
                                }
                            }
                        };
                    Ok(Some(next_page))
                })??;

        if let Some(page) = next_location {
            self.breadcrumbs
                .push((self.current_page, page.clone()));
            self.current_page = page.offset as usize;
            return Ok(false);
        }
        Ok(true)
    }

    /// Retrieves list of [`KeyCell`] starting from logical `start_index` up till
    /// `end_index`. This function returns [R_start..R_end).
    ///
    /// ## Panics
    ///
    /// This function will panic if the range(start_index..end_index) is not valid
    /// for [`Page`] `num_keys`.
    fn key_range(
        &self,
        start_index: usize,
        end_index: usize,
        page: &Page,
    ) -> io::Result<Vec<KeyCell>> {
        let start_offset = HEADER_SIZE + (start_index * KEYCELL_SIZE);
        let end_offset = HEADER_SIZE + (end_index * KEYCELL_SIZE);

        let mut bytes = vec![0; end_offset - start_offset];
        let mut out = Vec::new();
        bytes.copy_from_slice(page.cell(start_offset, end_offset));

        let mut keys = bytes
            .chunks(KEYCELL_SIZE)
            .map(KeyCell::from_bytes)
            .collect::<Vec<io::Result<_>>>();

        for r in keys.drain(..) {
            out.push(r?);
        }

        Ok(out)
    }
}

#[cfg(test)]
mod test {
    use tempfile::TempDir;

    use crate::{AccessContext, KEYCELL_SIZE, KeyCell, ValueCell, btree::Tree};

    fn temp_tree() -> (TempDir, Tree) {
        let dir = TempDir::new().expect("can create tempdir");
        let path = dir.path().join("store.db");
        let tree = Tree::load(path, 8).expect("can load tree");

        (dir, tree)
    }

    fn filled_leaf_root() -> (TempDir, Tree) {
        let (dir, tree) = temp_tree();

        let root = tree
            .inner
            .root()
            .expect("can retrieve tree root");
        assert!(root != 0, "root should be a valid page ID");
        tree.inner
            .pager
            .mut_page(
                root,
                AccessContext::maintenance("test leaf split"),
                |p| {
                    let mut initial_start = p.free_space_start() as usize;
                    let mut initial_end = p.free_space_end() as usize;

                    p.set_free_space(0);
                    p.set_num_keys(4);

                    let sample = [
                        (5, "asb"),
                        (20, "230"),
                        (50, "sdafjl"),
                        (90, "assdfj"),
                    ];
                    let sample = sample
                        .iter()
                        .map(|s| ValueCell {
                            key: s.0,
                            value: s.1.as_bytes().into(),
                        })
                        .map(|r| (r.key, Into::<Box<[u8]>>::into(&r)))
                        .collect::<Vec<_>>();

                    for (key, v) in sample {
                        p.mut_cell(initial_end - v.len(), initial_end)
                            .copy_from_slice(&v);
                        initial_end = initial_end - v.len();

                        let key = KeyCell {
                            key: key,
                            offset: initial_end as u32,
                        };
                        p.mut_cell(initial_start, initial_start + KEYCELL_SIZE)
                            .copy_from_slice(
                                Into::<[u8; KEYCELL_SIZE]>::into(&key).as_ref(),
                            );
                        initial_start += KEYCELL_SIZE;
                    }

                    p.set_free_space_end(initial_start as u16);
                },
            )
            .expect("can mutate page");

        (dir, tree)
    }

    #[test]
    fn cursor_search_success_on_not_found() {
        let (_dir, tree) = temp_tree();
        let mut cursor = tree
            .cursor()
            .expect("can initialize cursor");

        let key: u32 = 99;
        let result = cursor
            .search(&key)
            .expect("can search tree");
        assert!(result.is_none())
    }

    #[test]
    fn cursor_insert_can_insert_into_new() {
        let (_dir, tree) = temp_tree();

        let records: [(u32, &str); 3] = [(10, "abc"), (20, "nas"), (5, "mna")];
        for r in records {
            let mut cursor = tree
                .cursor()
                .expect("can initialize cursor");
            cursor
                .insert(
                    AccessContext::maintenance("cursor insert test"),
                    &r.0,
                    r.1.as_bytes().to_vec(),
                )
                .expect("can insert record");
        }

        records.iter().for_each(|r| {
            let mut cursor = tree
                .cursor()
                .expect("can initialize cursor");

            let found = cursor
                .search(&r.0)
                .expect("can search")
                .expect("record is located");

            assert_eq!(found.key, r.0);
            assert_eq!(found.value.as_ref(), r.1.as_bytes());
        });
    }

    #[test]
    fn cursor_insert_overrides_on_existing() {
        let (_dir, tree) = temp_tree();

        let record_1: (u32, &str) = (10, "abc");
        let record_2: (u32, &str) = (10, "lorem ipsum");

        let out = tree
            .cursor()
            .expect("can init cursor")
            .insert(
                AccessContext::maintenance("cursor insert overwrite"),
                &record_1.0,
                record_1.1.as_bytes().to_vec(),
            )
            .expect("can insert");

        assert!(out.is_none());

        let replaced = tree
            .cursor()
            .expect("can init cursor")
            .insert(
                AccessContext::maintenance("cursor insert overwrite"),
                &record_2.0,
                record_2.1.as_bytes().to_vec(),
            )
            .expect("can insert");

        assert!(replaced.is_some());
        let actual = replaced.unwrap();
        assert_eq!(actual.key, record_1.0);
        assert_eq!(actual.value.as_ref(), record_1.1.as_bytes());

        let updated = tree
            .cursor()
            .expect("can init cursor")
            .search(&record_2.0)
            .expect("can search tree");
        assert!(updated.is_some());

        let actual = updated.unwrap();
        assert_eq!(actual.key, record_2.0);
        assert_eq!(actual.value.as_ref(), record_2.1.as_bytes());
    }

    #[test]
    fn cursor_insert_splits_tree_if_needed() {
        let (_dir, tree) = filled_leaf_root();
        let record: (u32, &str) = (10, "abc");

        tree.cursor()
            .expect("can init cursor")
            .insert(
                AccessContext::maintenance("cursor insert test"),
                &record.0,
                record.1.as_bytes().to_vec(),
            )
            .expect("can insert into filled leaf");
    }
}
