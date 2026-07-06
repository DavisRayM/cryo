pub mod cursor;

use std::{io, path::PathBuf, sync::Arc};

pub use cursor::Cursor;

use crate::{AccessContext, PageFlags, Pager, pager::META_PAGE_ID};

/// [Tree] is a wrapping structure that signifies a `Blink-Tree` index-organized
/// table that can be traversed by [`Cursor`].
pub struct Tree {
    inner: Arc<TreeInner>,
}

/// Provides standardized access for [`Cursor`] to navigate the [`Tree`]
pub struct TreeInner {
    pager: Pager,
}

impl TreeInner {
    /// Returns the root of the [`Tree`]
    pub fn root(&self) -> io::Result<usize> {
        Ok(self.pager.page(
            META_PAGE_ID,
            AccessContext::maintenance("btree locate root"),
            |p| p.tree_root(),
        )? as usize)
    }

    /// Set current tree root to `root`
    pub fn set_root(&self, ctx: AccessContext, root: u32) -> io::Result<()> {
        self.pager
            .mut_page(META_PAGE_ID, ctx, |page| {
                page.set_tree_root(root);
            })
    }

    /// Creates a new B-Tree root page and updates the meta page to point to it.
    ///
    /// The old root page is not updated in any way. It's the responsibility of
    /// the caller to do so.
    pub fn create_root(
        &self,
        ctx: AccessContext,
        flags: PageFlags,
    ) -> io::Result<()> {
        let new_root = self
            .pager
            .allocate_page(ctx, flags | PageFlags::IsRoot)?;
        self.set_root(ctx, new_root as u32)?;
        Ok(())
    }
}

impl Tree {
    /// Load/Create a [`Tree`] at the given `path`. Initiating table meta page
    /// and tree root if not initiated.
    pub fn load(
        path: impl Into<PathBuf>,
        cache_size: usize,
    ) -> io::Result<Self> {
        let tree = Self {
            inner: Arc::new(TreeInner {
                pager: Pager::open(path, cache_size)?,
            }),
        };

        let root = tree.inner.pager.page(
            META_PAGE_ID,
            AccessContext::maintenance("load tree from disk"),
            |p| p.tree_root(),
        )?;

        if root == 0 {
            tree.inner.create_root(
                AccessContext::maintenance("initialize tree"),
                PageFlags::IsLeaf,
            )?;
        }

        Ok(tree)
    }

    /// Creates a new [`Cursor`] that can be used to traverse
    /// the tree.
    pub fn cursor(&self) -> io::Result<Cursor> {
        Cursor::from_root(&self.inner)
    }
}
