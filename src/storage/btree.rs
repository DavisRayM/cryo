use std::{path::PathBuf, sync::Arc};

use super::{
    AccessContext, Cursor, MetaPage, Page, PageFlags, Pager, StorageError,
    TablePage,
    constants::page::META_PAGE_ID,
    error::Result,
    page::{AnyPage, AnyPageMut},
};

/// [Tree] is a wrapping structure that signifies a `Blink-Tree` index-organized
/// table that can be traversed by [`Cursor`].
pub struct Tree {
    pub(crate) inner: Arc<TreeInner>,
}

/// Provides standardized access for [`Cursor`] to navigate the [`Tree`]
pub(crate) struct TreeInner {
    pager: Pager,
}

impl TreeInner {
    /// Returns the root of the [`Tree`]
    pub fn root(&self) -> Result<usize> {
        self.meta_page(
            AccessContext::maintenance("tree locate root page id"),
            |p| p.tree_root() as usize,
        )
    }

    /// Set current tree root to `root`
    pub fn set_root(&self, ctx: AccessContext, root: u32) -> Result<()> {
        self.mut_meta_page(ctx, |mut p| {
            p.set_tree_root(root);
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
    ) -> Result<()> {
        let new_root = self
            .pager
            .allocate_page(ctx, flags | PageFlags::IsRoot)?;
        self.set_root(ctx, new_root as u32)?;
        Ok(())
    }
}

impl TreeInner {
    pub fn meta_page<R>(
        &self,
        ctx: AccessContext,
        f: impl FnOnce(MetaPage<&Page>) -> R,
    ) -> Result<R> {
        self.pager
            .page(META_PAGE_ID, ctx, |p| match p {
                AnyPage::Meta(p) => Ok(f(p)),
                _ => Err(StorageError::CorruptedData(format!(
                    "expected metadata page at ID {}",
                    META_PAGE_ID
                ))),
            })?
    }

    pub fn mut_meta_page<R>(
        &self,
        ctx: AccessContext,
        f: impl FnOnce(MetaPage<&mut Page>) -> R,
    ) -> Result<R> {
        self.pager
            .mut_page(META_PAGE_ID, ctx, |p| match p {
                AnyPageMut::Meta(p) => Ok(f(p)),
                _ => Err(StorageError::CorruptedData(format!(
                    "expected metadata page at ID {}",
                    META_PAGE_ID
                ))),
            })?
    }

    pub fn table_page<R>(
        &self,
        ctx: AccessContext,
        page_id: usize,
        f: impl FnOnce(TablePage<&Page>) -> R,
    ) -> Result<R> {
        self.pager
            .page(page_id, ctx, |p| match p {
                AnyPage::Table(p) => Ok(f(p)),
                _ => Err(StorageError::CorruptedData(format!(
                    "expected table page at ID {}",
                    META_PAGE_ID
                ))),
            })?
    }

    pub fn mut_table_page<R>(
        &self,
        ctx: AccessContext,
        page_id: usize,
        f: impl FnOnce(TablePage<&mut Page>) -> R,
    ) -> Result<R> {
        self.pager
            .mut_page(page_id, ctx, |p| match p {
                AnyPageMut::Table(p) => Ok(f(p)),
                _ => Err(StorageError::CorruptedData(format!(
                    "expected table page at ID {}",
                    META_PAGE_ID
                ))),
            })?
    }
}

impl Tree {
    /// Load/Create a [`Tree`] at the given `path`. Initiating table meta page
    /// and tree root if not initiated.
    pub fn load(path: impl Into<PathBuf>, cache_size: usize) -> Result<Self> {
        let tree = Self {
            inner: Arc::new(TreeInner {
                pager: Pager::open(path, cache_size)?,
            }),
        };

        let root = tree.inner.root()?;
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
    pub fn cursor(&self) -> Result<Cursor> {
        Cursor::from_root(&self.inner)
    }
}
