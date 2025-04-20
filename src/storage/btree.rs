//! Disk-based BTree implementation for ordered row storage.
//!
//! This module implements a persistent **B-Tree** structure used by Cryo's storage engine
//! to index and retrieve rows in sorted order. The B-Tree is built on top of the
//! [`Pager`], using fixed-size [`Page`] structures to represent internal and leaf nodes.
//!
//! It supports efficient insertion, lookup, and  deletion operations, while
//! maintaining balance and minimizing disk I/O by organizing data in a tree of pages.
//!
//! # Design Overview
//! - **Internal pages** contain keys and child page IDs
//! - **Leaf pages** store actual [`Row`](crate::storage::row::Row) entries
//! - Pages are loaded and persisted via the pager
//! - Insertions may cause **page splits**, which propagate up the tree
//!
//! # Current Capabilities
//! - Insert records while maintaining sorted order
//! - Retrieve values by key using binary search per page
//! - Automatically split pages when full
//! - Delete values and merge pages if neccessary
//!
//! # Example
//! ```rust
//! use cryo::storage::btree::BTree;
//! use cryo::storage::pager::Pager;
//!
//! let mut pager = Pager::open("btree.db".into()).unwrap();
//! let mut tree = BTree::new(pager);
//! ```
//!
//! # Future Features
//! - Range scans / iteration across multiple pages
//! - Optimizations for bulk inserts or compaction
//!
//! # See Also
//! - [`Page`]: Core unit of storage used to build B-Tree nodes
//! - [`Row`]: Key-value data stored in leaf pages
//! - [`Pager`]: Manages disk I/O and caching for pages
//! - [`StorageEngine`]: Exposes a high-level interface backed by the B-Tree

use std::collections::VecDeque;

use log::{debug, error, trace};

use crate::storage::{EngineAction, PageError, row::RowType};

use super::{
    Row, StorageError,
    page::{Page, PageType, ROW_SPACE},
    pager::Pager,
};

/// Disk-based BTree Implementation
#[derive(Debug)]
pub struct BTree {
    /// Strores (page_id, pointer_idx) of previously traversed nodes.
    breadcrumbs: Vec<(usize, usize)>,
    /// Tracks the current position of the cursor in the BTree.
    current: usize,
    /// Pager instance used for page management.
    pager: Pager,
    /// ID of the current root page.
    root: usize,
}

impl BTree {
    /// Creates a new BTree instance
    pub fn new(mut pager: Pager) -> Result<Self, StorageError> {
        let mut root;
        if pager.pages == 0 {
            root = pager.allocate()?;
            let mut page = Page::new(PageType::Leaf, None, vec![], 0);
            pager.write(root, &mut page)?;
        } else {
            root = 0;
            while let Some(parent) = pager.read(root)?.parent {
                root = parent;
            }
        }

        Ok(Self {
            breadcrumbs: vec![],
            current: root,
            pager,
            root,
        })
    }

    /// Selects all leaf rows present in the BTree.
    pub fn select(&mut self) -> Result<Vec<Row>, StorageError> {
        let mut stack = VecDeque::from([self.root]);
        let mut out = Vec::new();
        let mut visited = Vec::new();

        #[allow(clippy::manual_while_let_some)]
        while !stack.is_empty() {
            let id = stack.pop_back().expect("stack is not empty");
            if visited.contains(&id) {
                continue;
            }

            let target = self.pager.read(id)?;
            visited.push(id);

            if target._type == PageType::Leaf {
                out.extend_from_slice(&target.select()[..]);
            } else {
                target.select().iter().for_each(|r| {
                    stack.push_front(r.left_offset());
                    stack.push_front(r.right_offset());
                });
            }
        }

        Ok(out)
    }

    /// Deletes a row if present in the BTree structure
    ///
    /// # Errors
    ///
    /// Errors out if the row does not exist in the structure.
    pub fn delete(&mut self, row: Row) -> Result<(), StorageError> {
        self.locate_row(&row)?;

        let mut page = self.pager.read(self.current)?;
        page.delete(row)?;
        self.pager.write(self.current, &mut page)?;

        // Only merge child nodes
        if self.current != self.root {
            self.attempt_merge()?;
        }

        Ok(())
    }

    /// Updates a row if present in the BTree structure
    ///
    /// # Errors
    ///
    /// Errors out if the row does not exist in the structure.
    pub fn update(&mut self, row: Row) -> Result<Row, StorageError> {
        self.locate_row(&row)?;
        debug!("updating row {} in page {}", row.id(), self.current);

        let mut page = self.pager.read(self.current)?;
        let out = page.update(row)?;

        self.pager.write(self.current, &mut page)?;
        Ok(out)
    }

    /// Adds a new row entry into the BTree structure.
    ///
    /// # Errors
    ///
    /// Errors out if row already exists in the structure.
    pub fn insert(&mut self, row: Row) -> Result<(), StorageError> {
        self.locate_row(&row)?;
        let mut page = self.pager.read(self.current)?;
        debug!("inserting row {} in page {}", row.id(), self.current);

        match page.insert(row.clone()) {
            Ok(_) => {
                self.pager.write(self.current, &mut page)?;
                debug!(
                    "row {} successfully inserted in page {}",
                    row.id(),
                    self.current
                );
                Ok(())
            }
            Err(StorageError::Page {
                cause: PageError::Full,
            }) => {
                debug!("current page is at maximum capacity");
                self.split(row)
            }
            Err(e) => {
                debug!("error during insert: {}", e);
                Err(StorageError::Engine {
                    action: EngineAction::Insert,
                    cause: Box::new(e),
                })
            }
        }
    }

    /// Locates the most likely position of a row in the BTree structure. Modifies
    /// `self.current` to point to the likely location.
    ///
    /// NOTE: Path taken by the BTree can be tracked using `self.breadcrumbs`
    fn locate_row(&mut self, row: &Row) -> Result<(), StorageError> {
        self.current = self.root;
        self.breadcrumbs.clear();

        debug!("searching for position of row {}", row.id(),);
        loop {
            let page = self.pager.read(self.current)?;

            if page._type != PageType::Leaf {
                self.search_internal(row)?;
                continue;
            }

            break Ok(());
        }
    }

    /// Searches the BTree internal node for the most likely location of a row. Modifies
    /// `self.current` to point to the likely location.
    ///
    /// # Panics
    ///
    /// This functions panics if called while `self.current` points to a leaf node
    fn search_internal(&mut self, row: &Row) -> Result<(), StorageError> {
        let page = self.pager.read(self.current)?;
        if page._type == PageType::Leaf {
            error!("attempted to search leaf node {}", self.current);
            panic!("search operation not supported");
        }

        debug!("searching internal node {} for {}", self.current, row.id());
        let pointers = page.select();
        trace!(
            "internal pointers: {:?}",
            pointers.iter().map(|r| format!(
                "[Left: {}, Right: {}]",
                r.left_offset(),
                r.right_offset()
            ))
        );

        let page_id = match pointers.binary_search(row) {
            Ok(pos) => {
                self.breadcrumbs.push((self.current, pos));
                pointers[pos].right_offset()
            }
            Err(pos) => {
                let pointer = if pos == pointers.len() {
                    self.breadcrumbs.push((self.current, pos - 1));
                    &pointers[pos - 1]
                } else {
                    self.breadcrumbs.push((self.current, pos));
                    &pointers[pos]
                };
                if pointer.id() >= row.id() {
                    pointer.left_offset()
                } else {
                    pointer.right_offset()
                }
            }
        };
        debug!("possible row position in page {page_id}");
        self.current = page_id;
        Ok(())
    }

    /// Attemps to merge the current node; checks if the combined
    /// size of the node and sibling are enough to fit into one node and merges
    /// them.
    ///
    /// # Panics
    ///
    /// Panics if `self.breadcrumbs is empty`
    fn attempt_merge(&mut self) -> Result<(), StorageError> {
        debug!("checking if page {} can be merged", self.current);
        let page = self.pager.read(self.current)?;
        let (parent_id, pointer_pos) = self
            .breadcrumbs
            .pop()
            .expect("breadcrumbs should track traversal path");
        let pointers = self.pager.read(parent_id)?.select();
        let pointer = &pointers[pointer_pos];

        let (sucessor, ancestor, sibling) = if pointer.left_offset() == self.current {
            (
                self.current,
                pointer.right_offset(),
                self.pager.read(pointer.right_offset()),
            )
        } else {
            (
                pointer.left_offset(),
                self.current,
                self.pager.read(pointer.left_offset()),
            )
        };

        if page.size + sibling?.size <= ROW_SPACE {
            self.merge(sucessor, ancestor, pointer_pos)?;
        }

        Ok(())
    }

    /// Merges the sibling node into the target node and updates
    /// the pointer on the parent node.
    fn merge(
        &mut self,
        successor_id: usize,
        ancestor_id: usize,
        parent_pointer: usize,
    ) -> Result<(), StorageError> {
        debug!("merging page {} into {}", ancestor_id, successor_id);
        let mut successor = self.pager.read(successor_id)?;
        let ancestor = self.pager.read(ancestor_id)?;
        let parent_id = successor.parent.expect("merge called on root node");
        let mut parent = self.pager.read(parent_id)?;
        let mut moved_rows = ancestor.select();
        let mut pointers = parent.select();

        while let Some(row) = moved_rows.pop() {
            successor.insert(row)?;
        }
        self.pager.write(successor_id, &mut successor)?;
        // TODO: Free the ancestor page

        let (left_pointer, delete_pointer, right_pointer) =
            if parent_pointer > 0 && parent_pointer + 1 < pointers.len() {
                let remove_pos = parent_pointer - 1;
                (
                    Some(pointers.remove(remove_pos)),
                    pointers.remove(remove_pos),
                    Some(pointers.remove(remove_pos)),
                )
            } else if parent_pointer > 0 {
                let remove_pos = parent_pointer - 1;
                (
                    Some(pointers.remove(remove_pos)),
                    pointers.remove(remove_pos),
                    None,
                )
            } else if parent_pointer + 1 < pointers.len() {
                (
                    None,
                    pointers.remove(parent_pointer),
                    Some(pointers.remove(parent_pointer)),
                )
            } else {
                (None, pointers.remove(parent_pointer), None)
            };

        parent.delete(delete_pointer)?;
        // Update pointers in parent node
        if let Some(mut left) = left_pointer {
            debug!(
                "updating (left) pointer {} in page {}",
                left.id(),
                parent_id
            );
            left.set_right_offset(successor_id);
            parent.update(left)?;
        }

        if let Some(mut right) = right_pointer {
            debug!(
                "updating (right) pointer {} in page {}",
                right.id(),
                parent_id
            );
            right.set_left_offset(successor_id);
            parent.update(right)?;
        }
        self.pager.write(parent_id, &mut parent)?;

        // Propagate merge upwards
        if parent.size == 0 && parent_id == self.root {
            self.root = successor_id;
            self.current = successor_id;
            Ok(())
        } else {
            self.current = parent_id;
            self.attempt_merge()
        }
    }

    /// Splits the BTree node at `self.current` and inserts a row
    /// into the appropriate node after split.
    fn split(&mut self, row: Row) -> Result<(), StorageError> {
        if self.current == self.root {
            // Create new root internal node page
            let parent_id = self.pager.allocate()?;
            let mut current = self.pager.read(self.current)?;
            current.parent = Some(parent_id);
            self.pager.write(self.current, &mut current)?;

            // Split child and retrieve internal row pointer to new children
            let pointer = self.split_node(self.current, row)?;
            let mut parent = Page::new(PageType::Internal, None, vec![pointer], 0);

            self.pager.write(parent_id, &mut parent)?;

            self.root = parent_id;
            self.current = parent_id;
            Ok(())
        } else if let Some(parent_id) = self.pager.read(self.current)?.parent {
            // Split child and retrieve internal row pointer to new children
            let pointer = self.split_node(self.current, row)?;
            self.current = parent_id;

            let mut parent = self.pager.read(parent_id)?;
            match parent.insert(pointer.clone()) {
                Ok(()) => {
                    self.pager.write(parent_id, &mut parent)?;
                    Ok(())
                }
                Err(StorageError::Page {
                    cause: PageError::Full,
                }) => {
                    self.split(pointer)?;
                    Ok(())
                }
                Err(e) => Err(StorageError::Engine {
                    action: EngineAction::Split,
                    cause: Box::new(e),
                }),
            }
        } else {
            panic!("split called on a node that's not root and does not have a parent")
        }
    }

    /// Splits a target node into two, inserting a row into one of the newly split nodes.
    /// Returns the pointer to the split nodes.
    ///
    /// # Panics
    ///
    /// This function panics if the target has no parent pointer
    fn split_node(&mut self, id: usize, insert_row: Row) -> Result<Row, StorageError> {
        let mut target = self.pager.read(id)?;
        let is_leaf = target._type == PageType::Leaf;

        let parent_id = target.parent.expect("missing parent pointer");
        let right_id = self.pager.allocate()?;
        let mut parent = self.pager.read(parent_id)?;

        let mut left_candidates = target.select();
        let splitat = left_candidates.len() / 2;
        let right_candidates = left_candidates.split_off(splitat);

        let mut pointer = Row::new(right_candidates[0].id(), RowType::Internal);
        pointer.set_left_offset(id);
        pointer.set_right_offset(right_id);

        if !is_leaf {
            for pointer in right_candidates.iter() {
                let page_id = pointer.left_offset();
                let mut page = self.pager.read(page_id)?;
                page.parent = Some(right_id);
                self.pager.write(page_id, &mut page)?;

                let page_id = pointer.right_offset();
                let mut page = self.pager.read(page_id)?;
                page.parent = Some(right_id);
                self.pager.write(page_id, &mut page)?;
            }
        }

        let right_size: usize = right_candidates.iter().map(|r| r.as_bytes().len()).sum();
        let left_size: usize = left_candidates.iter().map(|r| r.as_bytes().len()).sum();
        let mut right = Page::new(target._type, target.parent, right_candidates, right_size);
        target.size = left_size;
        target.rows = left_candidates;

        if insert_row.id() >= pointer.id() {
            if !is_leaf {
                // Update the links target page to point to the correct parent
                let mut page_id = insert_row.left_offset();
                let mut page = self.pager.read(page_id)?;
                page.parent = Some(right_id);
                self.pager.write(page_id, &mut page)?;

                page_id = insert_row.right_offset();
                page = self.pager.read(page_id)?;
                page.parent = Some(right_id);
                self.pager.write(page_id, &mut page)?;
            }
            right.insert(insert_row)?;
        } else {
            target.insert(insert_row)?;
        }

        self.pager.write(parent_id, &mut parent)?;
        self.pager.write(id, &mut target)?;
        self.pager.write(right_id, &mut right)?;

        Ok(pointer)
    }
}

#[cfg(test)]
mod tests {
    use tempdir::TempDir;

    use crate::{
        storage::row::{ROW_USERNAME_SIZE, RowType},
        utilities::{USERNAME_MAX_LENGTH, char_to_byte, extend_char_array},
    };

    use super::*;

    #[test]
    fn btree_new_empty() {
        let temp = TempDir::new("BTreeConstruction").unwrap();
        let pager = Pager::open(temp.into_path().join("cryo.db")).unwrap();
        let tree = BTree::new(pager).unwrap();

        assert_eq!(tree.root, 0);
    }

    #[test]
    fn btree_new_existing_tree() {
        let temp = TempDir::new("BTreeConstruction").unwrap();
        let mut pager = Pager::open(temp.into_path().join("cryo.db")).unwrap();

        let left = pager.allocate().unwrap();
        let right = pager.allocate().unwrap();
        let root = pager.allocate().unwrap();

        let mut page = Page::new(PageType::Leaf, Some(root), vec![], 0);
        pager.write(left, &mut page).unwrap();
        page = Page::new(PageType::Leaf, Some(root), vec![], 0);
        pager.write(right, &mut page).unwrap();
        page = Page::new(PageType::Internal, None, vec![], 0);
        pager.write(root, &mut page).unwrap();

        let tree = BTree::new(pager).unwrap();
        assert_eq!(tree.root, root);
    }

    #[test]
    fn btree_insert_empty() {
        let temp = TempDir::new("BTreeInsert").unwrap();
        let pager = Pager::open(temp.into_path().join("cryo.db")).unwrap();
        let mut tree = BTree::new(pager).unwrap();

        let row = Row::new(1, RowType::Leaf);
        tree.insert(row).unwrap();
    }

    #[test]
    fn btree_insert_multilevel() {
        let temp = TempDir::new("BTreeInsert").unwrap();
        let mut pager = Pager::open(temp.into_path().join("cryo.db")).unwrap();

        let left = pager.allocate().unwrap();
        let right = pager.allocate().unwrap();
        let root = pager.allocate().unwrap();

        let mut page = Page::new(PageType::Leaf, Some(root), vec![], 0);
        pager.write(left, &mut page).unwrap();
        page = Page::new(PageType::Leaf, Some(root), vec![], 0);
        pager.write(right, &mut page).unwrap();

        let mut row = Row::new(4, RowType::Internal);
        row.set_left_offset(left);
        row.set_right_offset(right);
        page = Page::new(PageType::Internal, None, vec![row], 0);
        pager.write(root, &mut page).unwrap();

        let mut tree = BTree::new(pager).unwrap();
        let insert = Row::new(1, RowType::Leaf);
        tree.insert(insert).unwrap();
    }

    #[test]
    fn btree_insert_split() {
        let temp = TempDir::new("BTreeInsert").unwrap();
        let mut pager = Pager::open(temp.into_path().join("cryo.db")).unwrap();

        let mut page = Page::new(PageType::Leaf, None, vec![], 0);
        for i in 1..1000 {
            let row = Row::new(i, RowType::Leaf);
            if let Err(StorageError::Page {
                cause: PageError::Full,
            }) = page.insert(row)
            {
                break;
            }
        }
        let root = pager.allocate().unwrap();
        pager.write(root, &mut page).unwrap();

        let mut tree = BTree::new(pager).unwrap();
        let row = Row::new(1001, RowType::Leaf);
        tree.insert(row).unwrap();

        let row = Row::new(0, RowType::Leaf);
        tree.insert(row).unwrap();
    }

    #[test]
    fn btree_insert_split_child() {
        let temp = TempDir::new("BTreeInsert").unwrap();
        let mut pager = Pager::open(temp.into_path().join("cryo.db")).unwrap();
        let root = pager.allocate().unwrap();

        let mut page = Page::new(PageType::Leaf, Some(root), vec![], 0);
        for i in 1..1000 {
            let row = Row::new(i, RowType::Leaf);
            if let Err(StorageError::Page {
                cause: PageError::Full,
            }) = page.insert(row)
            {
                break;
            }
        }
        let left = pager.allocate().unwrap();
        pager.write(left, &mut page).unwrap();

        page = Page::new(PageType::Leaf, Some(root), vec![], 0);
        for i in 1000..2000 {
            let row = Row::new(i, RowType::Leaf);
            if let Err(StorageError::Page {
                cause: PageError::Full,
            }) = page.insert(row)
            {
                break;
            }
        }
        let right = pager.allocate().unwrap();
        pager.write(right, &mut page).unwrap();

        let mut pointer = Row::new(1000, RowType::Internal);
        pointer.set_left_offset(left);
        pointer.set_right_offset(right);
        let size = pointer.as_bytes().len();
        page = Page::new(PageType::Internal, None, vec![pointer], size);
        pager.write(root, &mut page).unwrap();

        let mut tree = BTree::new(pager).unwrap();
        let row = Row::new(2001, RowType::Leaf);
        tree.insert(row).unwrap();

        let row = Row::new(0, RowType::Leaf);
        tree.insert(row).unwrap();
    }

    #[test]
    fn btree_select() {
        let temp = TempDir::new("BTreeInsert").unwrap();
        let pager = Pager::open(temp.into_path().join("cryo.db")).unwrap();
        let mut tree = BTree::new(pager).unwrap();
        let mut expected = Vec::new();

        for i in 1..10 {
            let row = Row::new(i, RowType::Leaf);
            expected.push(row.clone());
            tree.insert(row).unwrap();
        }

        assert_eq!(tree.select().unwrap(), expected);
    }

    #[test]
    fn btree_ordering() {
        let temp = TempDir::new("BTreeInsert").unwrap();
        let pager = Pager::open(temp.into_path().join("cryo.db")).unwrap();
        let mut tree = BTree::new(pager).unwrap();
        let mut expected = VecDeque::new();

        for i in 10..0 {
            let row = Row::new(i, RowType::Leaf);
            expected.push_front(row.clone());
            tree.insert(row).unwrap();
        }

        assert_eq!(
            tree.select().unwrap(),
            expected.into_iter().collect::<Vec<Row>>()
        );
    }

    #[test]
    fn btree_update() {
        let temp = TempDir::new("BTreeInsert").unwrap();
        let pager = Pager::open(temp.into_path().join("cryo.db")).unwrap();
        let mut tree = BTree::new(pager).unwrap();

        let mut row = Row::new(1, RowType::Leaf);
        tree.insert(row.clone()).unwrap();

        let username = vec!['t', 'e', 's', 't'];
        row.set_username(
            char_to_byte(
                extend_char_array::<USERNAME_MAX_LENGTH>(username, '\0')
                    .unwrap()
                    .as_ref(),
            )
            .as_slice()
            .try_into()
            .unwrap(),
        );
        let old = tree.update(row.clone()).unwrap();

        assert_eq!(tree.select().unwrap(), vec![row]);
        assert_eq!(old.username(), vec![0; ROW_USERNAME_SIZE]);
    }

    #[test]
    fn btree_select_empty() {
        let temp = TempDir::new("BTreeInsert").unwrap();
        let pager = Pager::open(temp.into_path().join("cryo.db")).unwrap();
        let mut tree = BTree::new(pager).unwrap();

        assert_eq!(tree.select().unwrap(), vec![]);
    }

    #[test]
    fn btree_delete() {
        let temp = TempDir::new("BTreeInsert").unwrap();
        let pager = Pager::open(temp.into_path().join("cryo.db")).unwrap();
        let mut tree = BTree::new(pager).unwrap();
        let row = Row::new(1, RowType::Leaf);
        tree.insert(row.clone()).unwrap();
        tree.delete(row).unwrap();

        assert_eq!(tree.select().unwrap(), vec![])
    }

    #[test]
    fn btree_delete_merge() {
        let temp = TempDir::new("BTreeInsert").unwrap();
        let mut pager = Pager::open(temp.into_path().join("cryo.db")).unwrap();
        let left = pager.allocate().unwrap();
        let right = pager.allocate().unwrap();
        let root = pager.allocate().unwrap();

        let mut page = Page::new(PageType::Leaf, Some(root), vec![], 0);
        pager.write(left, &mut page).unwrap();
        page = Page::new(PageType::Leaf, Some(root), vec![], 0);
        pager.write(right, &mut page).unwrap();

        let mut row = Row::new(4, RowType::Internal);
        row.set_left_offset(left);
        row.set_right_offset(right);
        page = Page::new(PageType::Internal, None, vec![row], 0);
        pager.write(root, &mut page).unwrap();

        let mut tree = BTree::new(pager).unwrap();
        let insert = Row::new(1, RowType::Leaf);
        tree.insert(insert).unwrap();
        let insert = Row::new(2, RowType::Leaf);
        tree.insert(insert.clone()).unwrap();

        tree.delete(insert).unwrap();
    }
}
