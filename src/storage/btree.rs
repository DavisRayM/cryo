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

use log::{debug, error, trace};

use crate::storage::{EngineAction, PageError};

use super::{
    Row, StorageError,
    page::{Page, PageType},
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

    /// Adds a new row entry into the BTree structure.
    ///
    /// # Errors
    ///
    /// Errors out if row already exists in the structure.
    pub fn insert(&mut self, row: Row) -> Result<(), StorageError> {
        self.current = self.root;

        loop {
            let mut page = self.pager.read(self.current)?;
            debug!("attempting to insert row in page {}", self.current);

            if page._type != PageType::Leaf {
                debug!("page {} is an internal node", self.current);
                self.search_internal(&row)?;
                continue;
            }

            debug!(
                "target insert page located; inserting row in page {}",
                self.current
            );

            break match page.insert(row) {
                Ok(_) => {
                    self.pager.write(self.current, &mut page)?;
                    debug!("row successfully inserted in page {}", self.current);
                    Ok(())
                }
                Err(StorageError::Page {
                    cause: PageError::Full,
                }) => {
                    debug!("current page is at maximum capacity");
                    todo!()
                }
                Err(e) => {
                    debug!("error during insert: {}", e);
                    Err(StorageError::Engine {
                        action: EngineAction::Insert,
                        cause: Box::new(e),
                    })
                }
            };
        }
    }

    /// Searches the BTree structure for the most likely location of a row. Modifies
    /// `self.current_page` to point to the likely location.
    ///
    /// # Panics
    ///
    /// This functions panics if called while `self.current` points to a leaf node
    fn search_internal(&mut self, row: &Row) -> Result<(), StorageError> {
        let mut page = self.pager.read(self.current)?;
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
}

#[cfg(test)]
mod tests {
    use tempdir::TempDir;

    use crate::storage::row::RowType;

    use super::*;

    #[test]
    fn btree_new_empty() {
        let temp = TempDir::new("PagerConstruction").unwrap();
        let pager = Pager::open(temp.into_path().join("cryo.db")).unwrap();
        let tree = BTree::new(pager).unwrap();

        assert_eq!(tree.root, 0);
    }

    #[test]
    fn btree_new_existing_tree() {
        let temp = TempDir::new("PagerConstruction").unwrap();
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
        let temp = TempDir::new("PagerConstruction").unwrap();
        let pager = Pager::open(temp.into_path().join("cryo.db")).unwrap();
        let mut tree = BTree::new(pager).unwrap();

        let row = Row::new(1, RowType::Leaf);
        tree.insert(row).unwrap();
    }

    #[test]
    fn btree_insert_multilevel() {
        let temp = TempDir::new("PagerConstruction").unwrap();
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
}
