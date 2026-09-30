// Copyright 2018 foundationdb-rs developers, https://github.com/Clikengo/foundationdb-rs/graphs/contributors
// Copyright 2013-2018 Apple, Inc and the FoundationDB project authors.
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

//! The default Directory implementation.

use crate::RangeOption;
use crate::directory::directory_partition::DirectoryPartition;
use crate::directory::directory_subspace::DirectorySubspace;
use crate::directory::error::DirectoryError;
use crate::directory::node::Node;
use crate::directory::{Directory, DirectoryOutput, compare_slice, strinc};
use crate::future::FdbSlice;
use crate::tuple::hca::HighContentionAllocator;
use crate::tuple::{Element, Subspace, TuplePack};
use crate::{FdbResult, Transaction};
use async_recursion::async_recursion;
use async_trait::async_trait;
use futures::TryStreamExt;
use std::cmp::Ordering;
use std::ops::Deref;
use std::sync::Arc;

pub(crate) const DEFAULT_SUB_DIRS: i64 = 0;
const MAJOR_VERSION: u32 = 1;
const MINOR_VERSION: u32 = 0;
const PATCH_VERSION: u32 = 0;
pub(crate) const DEFAULT_NODE_PREFIX: &[u8] = b"\xFE";
const DEFAULT_HCA_PREFIX: &[u8] = b"hca";
pub(crate) const PARTITION_LAYER: &[u8] = b"partition";
pub(crate) const LAYER_SUFFIX: &[u8] = b"layer";

/// A DirectoryLayer defines a new root directory.
/// The node subspace and content subspace control where the directory metadata and contents,
/// respectively, are stored. The default root directory has a node subspace with raw prefix \xFE
/// and a content subspace with no prefix.
#[derive(Clone)]
pub struct DirectoryLayer {
    pub(crate) inner: Arc<DirectoryLayerInner>,
}

impl std::fmt::Debug for DirectoryLayer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

#[derive(Debug)]
pub struct DirectoryLayerInner {
    pub(crate) root_node: Subspace,
    pub(crate) node_subspace: Subspace,
    pub(crate) content_subspace: Subspace,
    pub(crate) allocator: HighContentionAllocator,
    pub(crate) allow_manual_prefixes: bool,

    pub(crate) path: Vec<String>,
}

impl Deref for DirectoryLayer {
    type Target = DirectoryLayerInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl Default for DirectoryLayer {
    /// The default root directory stores directory layer metadata in keys beginning with 0xFE,
    ///and allocates newly created directories in (unused) prefixes starting with 0x00 through 0xFD.
    ///This is appropriate for otherwise empty databases, but may conflict with other formal or informal partitionings of keyspace.
    /// If you already have other content in your database, you may wish to use NewDirectoryLayer to
    /// construct a non-standard root directory to control where metadata and keys are stored.
    fn default() -> Self {
        Self::new(
            Subspace::from_bytes(DEFAULT_NODE_PREFIX),
            Subspace::all(),
            false,
        )
    }
}

impl DirectoryLayer {
    pub fn new(
        node_subspace: Subspace,
        content_subspace: Subspace,
        allow_manual_prefixes: bool,
    ) -> Self {
        let root_node = node_subspace.subspace(&node_subspace.bytes());
        let allocator = HighContentionAllocator::new(root_node.subspace(&DEFAULT_HCA_PREFIX));

        DirectoryLayer {
            inner: Arc::new(DirectoryLayerInner {
                root_node,
                node_subspace,
                content_subspace,
                allocator,
                allow_manual_prefixes,
                path: vec![],
            }),
        }
    }

    pub(crate) fn new_with_path(
        node_subspace: Subspace,
        content_subspace: Subspace,
        allow_manual_prefixes: bool,
        path: &[String],
    ) -> Self {
        let root_node = node_subspace.subspace(&node_subspace.bytes());
        let allocator = HighContentionAllocator::new(root_node.subspace(&DEFAULT_HCA_PREFIX));

        DirectoryLayer {
            inner: Arc::new(DirectoryLayerInner {
                root_node,
                node_subspace,
                content_subspace,
                allocator,
                allow_manual_prefixes,
                path: Vec::from(path),
            }),
        }
    }

    pub fn get_path(&self) -> &[String] {
        self.path.as_slice()
    }

    fn node_with_optional_prefix(&self, prefix: Option<FdbSlice>) -> Option<Subspace> {
        prefix.map(|fdb_slice| self.node_with_prefix(&fdb_slice.deref()))
    }

    fn node_with_prefix<T: TuplePack>(&self, prefix: &T) -> Subspace {
        self.inner.node_subspace.subspace(prefix)
    }

    async fn find(
        &self,
        trx: &Transaction,
        path: &[String],
    ) -> Result<Option<Node>, DirectoryError> {
        let mut current_path = vec![];
        let mut node_subspace = self.root_node.clone();
        let mut layer = vec![];
        let mut loaded = false;

        // walking through the provided path
        for path_name in path.iter() {
            current_path.push(path_name.clone());
            let key = node_subspace.subspace(&(DEFAULT_SUB_DIRS, path_name.to_owned()));

            // finding the next node
            let fdb_slice_value = trx.get(key.bytes(), false).await?;

            loaded = true;
            node_subspace = match self.node_with_optional_prefix(fdb_slice_value) {
                None => return Ok(None),
                Some(subspace) => subspace,
            };

            layer = Node::load_metadata(trx, &node_subspace).await?;
            if layer.as_slice().eq(PARTITION_LAYER) {
                break;
            }
        }

        if !loaded {
            layer = Node::load_metadata(trx, &node_subspace).await?;
        }

        Ok(Some(Node {
            subspace: node_subspace,
            current_path,
            target_path: Vec::from(path),
            directory_layer: self.clone(),
            layer,
        }))
    }

    fn to_absolute_path(&self, sub_path: &[String]) -> Vec<String> {
        let mut path: Vec<String> = Vec::with_capacity(self.path.len() + sub_path.len());

        path.extend_from_slice(&self.path);
        path.extend_from_slice(sub_path);

        path
    }

    pub(crate) fn contents_of_node(
        &self,
        subspace: &Subspace,
        path: &[String],
        layer: &[u8],
    ) -> Result<DirectoryOutput, DirectoryError> {
        let prefix: Vec<u8> = self.node_subspace.unpack(subspace.bytes())?;

        if layer.eq(PARTITION_LAYER) {
            Ok(DirectoryOutput::DirectoryPartition(
                DirectoryPartition::new(&self.to_absolute_path(path), prefix, self.clone()),
            ))
        } else {
            Ok(DirectoryOutput::DirectorySubspace(DirectorySubspace::new(
                &self.to_absolute_path(path),
                prefix,
                self,
                layer.to_owned(),
            )))
        }
    }

    /// `create_or_open_internal` is the function used to open and/or create a directory.
    #[async_recursion]
    async fn create_or_open_internal(
        &self,
        trx: &Transaction,
        path: &[String],
        prefix: Option<&'async_recursion [u8]>,
        layer: Option<&'async_recursion [u8]>,
        allow_create: bool,
        allow_open: bool,
    ) -> Result<DirectoryOutput, DirectoryError> {
        self.check_version(trx, false).await?;

        if prefix.is_some() && !self.allow_manual_prefixes {
            if self.path.is_empty() {
                return Err(DirectoryError::PrefixNotAllowed);
            }

            return Err(DirectoryError::CannotPrefixInPartition);
        }

        if path.is_empty() {
            return Err(DirectoryError::NoPathProvided);
        }

        if let Some(node) = self.find(trx, path).await? {
            if node.is_in_partition(false) {
                let sub_path = node.get_partition_subpath();
                match node.get_contents()? {
                    DirectoryOutput::DirectorySubspace(_) => unreachable!("already in partition"),
                    DirectoryOutput::DirectoryPartition(directory_partition) => {
                        let dir_space = directory_partition
                            .directory_subspace
                            .directory_layer
                            .create_or_open_internal(
                                trx,
                                &sub_path,
                                prefix,
                                layer,
                                allow_create,
                                allow_open,
                            )
                            .await?;
                        Ok(dir_space)
                    }
                }
            } else {
                self.open_internal(layer, &node, allow_open).await
            }
        } else {
            self.create_internal(trx, path, layer, prefix, allow_create)
                .await
        }
    }

    async fn open_internal(
        &self,
        layer: Option<&[u8]>,
        node: &Node,
        allow_open: bool,
    ) -> Result<DirectoryOutput, DirectoryError> {
        if !allow_open {
            return Err(DirectoryError::DirAlreadyExists);
        }

        match layer {
            None => {}
            Some(layer) if !layer.is_empty() => match compare_slice(layer, &node.layer) {
                Ordering::Equal => {}
                _ => {
                    return Err(DirectoryError::IncompatibleLayer);
                }
            },
            Some(_) => {}
        }

        node.get_contents()
    }

    async fn create_internal(
        &self,
        trx: &Transaction,
        path: &[String],
        layer: Option<&[u8]>,
        prefix: Option<&[u8]>,
        allow_create: bool,
    ) -> Result<DirectoryOutput, DirectoryError> {
        let path_last = path.last().ok_or(DirectoryError::NoPathProvided)?;

        if !allow_create {
            return Err(DirectoryError::DirectoryDoesNotExists);
        }

        let layer = layer.unwrap_or_default();

        self.check_version(trx, true).await?;
        let new_prefix = self.get_prefix(trx, prefix).await?;

        let is_prefix_free = self
            .is_prefix_free(trx, new_prefix.as_slice(), prefix.is_none())
            .await?;

        if !is_prefix_free {
            return Err(DirectoryError::DirectoryPrefixInUse);
        }

        let parent_node = self.get_parent_node(trx, path).await?;
        let node = self.node_with_prefix(&new_prefix);

        let key = parent_node.subspace(&(DEFAULT_SUB_DIRS, path_last));
        let key_layer = node.pack(&LAYER_SUFFIX);

        trx.set(key.bytes(), &new_prefix);
        trx.set(&key_layer, layer);

        self.contents_of_node(&node, path, layer)
    }

    async fn get_parent_node(
        &self,
        trx: &Transaction,
        path: &[String],
    ) -> Result<Subspace, DirectoryError> {
        match path.split_last() {
            None => Ok(self.root_node.clone()),
            Some((_, remains)) => {
                if remains.is_empty() {
                    return Ok(self.root_node.clone());
                }
                let parent = self
                    .create_or_open_internal(trx, remains, None, None, true, true)
                    .await?;

                Ok(self.node_with_prefix(&parent.bytes()?))
            }
        }
    }

    async fn is_prefix_free(
        &self,
        trx: &Transaction,
        prefix: &[u8],
        snapshot: bool,
    ) -> Result<bool, DirectoryError> {
        if prefix.is_empty() {
            return Ok(false);
        }

        let node = self.node_containing_key(trx, prefix, snapshot).await?;

        if node.is_some() {
            return Ok(false);
        }

        let mut range_option = RangeOption::from((
            self.node_subspace.pack(&prefix),
            self.node_subspace.pack(&strinc(prefix.to_vec())),
        ));
        range_option.limit = Some(1);

        let result = trx
            .get_ranges_keyvalues(range_option, snapshot)
            .try_next()
            .await?;

        Ok(result.is_none())
    }

    async fn node_containing_key(
        &self,
        trx: &Transaction,
        key: &[u8],
        snapshot: bool,
    ) -> Result<Option<Subspace>, DirectoryError> {
        if key.starts_with(self.node_subspace.bytes()) {
            return Ok(Some(self.root_node.clone()));
        }

        let mut key_after = key.to_vec();
        // pushing 0x00 to simulate keyAfter
        key_after.push(0);

        let range_end = self.node_subspace.pack(&key_after);

        let mut range_option = RangeOption::from((self.node_subspace.range().0, range_end));
        range_option.reverse = true;
        range_option.limit = Some(1);

        // checking range
        let fdb_value = trx
            .get_ranges_keyvalues(range_option, snapshot)
            .try_next()
            .await?;

        if let Some(fdb_key_value) = fdb_value {
            let previous_prefix: Vec<Element> = self.node_subspace.unpack(fdb_key_value.key())?;

            if let Some(Element::Bytes(previous_prefix)) = previous_prefix.first() {
                if key.starts_with(previous_prefix) {
                    return Ok(Some(self.node_with_prefix(previous_prefix)));
                };
            };
        }
        Ok(None)
    }

    async fn get_prefix(
        &self,
        trx: &Transaction,
        prefix: Option<&[u8]>,
    ) -> Result<Vec<u8>, DirectoryError> {
        match prefix {
            None => {
                // no prefix provided, allocating one
                let allocator = self.allocator.allocate(trx).await?;
                let subspace = self.content_subspace.subspace(&allocator);

                // checking range
                let mut range_option = RangeOption::from(subspace.range());
                range_option.limit = Some(1);

                let result = trx
                    .get_ranges_keyvalues(range_option, false)
                    .try_next()
                    .await?;

                if result.is_some() {
                    return Err(DirectoryError::PrefixNotEmpty);
                }

                Ok(subspace.into_bytes())
            }
            Some(v) => Ok(v.to_vec()),
        }
    }

    /// `check_version` is checking the Directory's version in FDB.
    async fn check_version(
        &self,
        trx: &Transaction,
        allow_creation: bool,
    ) -> Result<(), DirectoryError> {
        let version = self.get_version_value(trx).await?;
        match version {
            None => {
                if allow_creation {
                    self.initialize_directory(trx).await
                } else {
                    Ok(())
                }
            }
            Some(versions) => {
                if versions.len() < 12 {
                    return Err(DirectoryError::Version(
                        "incorrect version length".to_string(),
                    ));
                }
                let mut arr = [0u8; 4];
                arr.copy_from_slice(&versions[0..4]);
                let major: u32 = u32::from_le_bytes(arr);

                arr.copy_from_slice(&versions[4..8]);
                let minor: u32 = u32::from_le_bytes(arr);

                arr.copy_from_slice(&versions[8..12]);
                let patch: u32 = u32::from_le_bytes(arr);

                if major > MAJOR_VERSION {
                    let msg = format!(
                        "cannot load directory with version {major}.{minor}.{patch} using directory layer {MAJOR_VERSION}.{MINOR_VERSION}.{PATCH_VERSION}"
                    );
                    return Err(DirectoryError::Version(msg));
                }

                if minor > MINOR_VERSION {
                    let msg = format!(
                        "directory with version {major}.{minor}.{patch} is read-only when opened using directory layer {MAJOR_VERSION}.{MINOR_VERSION}.{PATCH_VERSION}"
                    );
                    return Err(DirectoryError::Version(msg));
                }

                Ok(())
            }
        }
    }

    /// `initialize_directory` is initializing the directory
    async fn initialize_directory(&self, trx: &Transaction) -> Result<(), DirectoryError> {
        let mut value = vec![];
        value.extend(&MAJOR_VERSION.to_le_bytes());
        value.extend(&MINOR_VERSION.to_le_bytes());
        value.extend(&PATCH_VERSION.to_le_bytes());
        let version_subspace: &[u8] = b"version";
        let directory_version_key = self.root_node.subspace(&version_subspace);
        trx.set(directory_version_key.bytes(), &value);

        Ok(())
    }

    async fn get_version_value(&self, trx: &Transaction) -> FdbResult<Option<FdbSlice>> {
        let version_subspace: &[u8] = b"version";
        let version_key = self.root_node.subspace(&version_subspace);

        trx.get(version_key.bytes(), false).await
    }

    async fn exists_internal(
        &self,
        trx: &Transaction,
        path: &[String],
    ) -> Result<bool, DirectoryError> {
        self.check_version(trx, false).await?;

        match self.find(trx, path).await? {
            None => Ok(false),
            Some(node) if node.is_in_partition(false) => {
                node.get_contents()?
                    .exists(trx, &node.get_partition_subpath())
                    .await
            }
            Some(_node) => Ok(true),
        }
    }

    async fn list_internal(
        &self,
        trx: &Transaction,
        path: &[String],
    ) -> Result<Vec<String>, DirectoryError> {
        self.check_version(trx, false).await?;

        let node = self
            .find(trx, path)
            .await?
            .ok_or(DirectoryError::PathDoesNotExists)?;
        if node.is_in_partition(true) {
            match node.get_contents()? {
                DirectoryOutput::DirectorySubspace(_) => unreachable!("already in partition"),
                DirectoryOutput::DirectoryPartition(directory_partition) => {
                    return directory_partition
                        .directory_subspace
                        .directory_layer
                        .list(trx, &node.get_partition_subpath())
                        .await;
                }
            };
        }

        node.list_sub_folders(trx).await
    }

    async fn move_to_internal(
        &self,
        trx: &Transaction,
        old_path: &[String],
        new_path: &[String],
    ) -> Result<DirectoryOutput, DirectoryError> {
        self.check_version(trx, true).await?;

        if old_path.len() <= new_path.len()
            && compare_slice(old_path, &new_path[..old_path.len()]) == Ordering::Equal
        {
            return Err(DirectoryError::CannotMoveBetweenSubdirectory);
        }

        let old_node = self.find(trx, old_path).await?;
        let new_node = self.find(trx, new_path).await?;

        let old_node = match old_node {
            None => return Err(DirectoryError::PathDoesNotExists),
            Some(n) => n,
        };
        let old_node_exists_in_partition = old_node.is_in_partition(false);

        match new_node {
            None => {
                if old_node_exists_in_partition {
                    return Err(DirectoryError::CannotMoveBetweenPartition);
                }
            }
            Some(new_node) => {
                let new_node_exists_in_partition = new_node.is_in_partition(false);
                if old_node_exists_in_partition || new_node_exists_in_partition {
                    if !old_node_exists_in_partition
                        || !new_node_exists_in_partition
                        || !old_node.current_path.eq(&new_node.current_path)
                    {
                        return Err(DirectoryError::CannotMoveBetweenPartition);
                    }

                    return new_node
                        .get_contents()?
                        .move_to(
                            trx,
                            &old_node.get_partition_subpath(),
                            &new_node.get_partition_subpath(),
                        )
                        .await;
                }
                return Err(DirectoryError::DirAlreadyExists);
            }
        }

        let (new_path_last, parent_path) = new_path
            .split_last()
            .ok_or(DirectoryError::DirAlreadyExists)?;

        let parent_node = self
            .find(trx, parent_path)
            .await?
            .ok_or(DirectoryError::ParentDirDoesNotExists)?;

        let key = parent_node
            .subspace
            .subspace(&(DEFAULT_SUB_DIRS, new_path_last));
        let value: Vec<u8> = self.node_subspace.unpack(old_node.subspace.bytes())?;
        trx.set(key.bytes(), &value);

        self.remove_from_parent(trx, old_path).await?;

        self.contents_of_node(&old_node.subspace, new_path, &old_node.layer)
    }

    async fn remove_from_parent(
        &self,
        trx: &Transaction,
        path: &[String],
    ) -> Result<(), DirectoryError> {
        let (last_element, parent_path) = path
            .split_last()
            .ok_or(DirectoryError::BadDestinationDirectory)?;

        match self.find(trx, parent_path).await? {
            None => {}
            Some(parent_node) => {
                let key = parent_node.subspace.pack(&(DEFAULT_SUB_DIRS, last_element));
                trx.clear(&key);
            }
        }

        Ok(())
    }

    #[async_recursion]
    async fn remove_internal(
        &self,
        trx: &Transaction,
        path: &[String],
        fail_on_nonexistent: bool,
    ) -> Result<bool, DirectoryError> {
        self.check_version(trx, true).await?;

        if path.is_empty() {
            return Err(DirectoryError::CannotModifyRootDirectory);
        }

        let node = match self.find(trx, path).await? {
            Some(node) => node,
            None if fail_on_nonexistent => return Err(DirectoryError::DirectoryDoesNotExists),
            None => return Ok(false),
        };

        if node.is_in_partition(false) {
            match node.get_contents()? {
                DirectoryOutput::DirectorySubspace(_) => {
                    unreachable!("already directory partition")
                }
                DirectoryOutput::DirectoryPartition(d) => {
                    return d
                        .directory_subspace
                        .directory_layer
                        .remove_internal(trx, &node.get_partition_subpath(), fail_on_nonexistent)
                        .await;
                }
            }
        }

        self.remove_recursive(trx, &node.subspace).await?;
        self.remove_from_parent(trx, path).await?;

        Ok(true)
    }

    #[async_recursion]
    async fn remove_recursive(
        &self,
        trx: &Transaction,
        node_sub: &Subspace,
    ) -> Result<(), DirectoryError> {
        let sub_dir = node_sub.subspace(&DEFAULT_SUB_DIRS);
        let range_option = RangeOption::from(&sub_dir);

        let mut stream = std::pin::pin!(trx.get_ranges_keyvalues(range_option, false));
        while let Some(row_key) = stream.try_next().await? {
            let sub_node = self.node_with_prefix(&row_key.value());
            self.remove_recursive(trx, &sub_node).await?;
        }

        let node_prefix: Vec<u8> = self.node_subspace.unpack(node_sub.bytes())?;

        trx.clear_range(&node_prefix, &strinc(node_prefix.to_owned()));
        trx.clear_subspace_range(node_sub);

        Ok(())
    }
}

#[async_trait]
impl Directory for DirectoryLayer {
    async fn create_or_open(
        &self,
        txn: &Transaction,
        path: &[String],
        prefix: Option<&[u8]>,
        layer: Option<&[u8]>,
    ) -> Result<DirectoryOutput, DirectoryError> {
        self.create_or_open_internal(txn, path, prefix, layer, true, true)
            .await
    }

    async fn create(
        &self,
        txn: &Transaction,
        path: &[String],
        prefix: Option<&[u8]>,
        layer: Option<&[u8]>,
    ) -> Result<DirectoryOutput, DirectoryError> {
        self.create_or_open_internal(txn, path, prefix, layer, true, false)
            .await
    }

    async fn open(
        &self,
        txn: &Transaction,
        path: &[String],
        layer: Option<&[u8]>,
    ) -> Result<DirectoryOutput, DirectoryError> {
        self.create_or_open_internal(txn, path, None, layer, false, true)
            .await
    }

    async fn exists(&self, trx: &Transaction, path: &[String]) -> Result<bool, DirectoryError> {
        self.exists_internal(trx, path).await
    }

    async fn move_directory(
        &self,
        _trx: &Transaction,
        _new_path: &[String],
    ) -> Result<DirectoryOutput, DirectoryError> {
        Err(DirectoryError::CannotMoveRootDirectory)
    }

    /// `move_to` the directory from old_path to new_path(both relative to this
    /// Directory), and returns the directory (at its new location) and its
    /// contents as a Subspace. Move will return an error if a directory
    /// does not exist at oldPath, a directory already exists at newPath, or the
    /// parent directory of newPath does not exist.
    async fn move_to(
        &self,
        trx: &Transaction,
        old_path: &[String],
        new_path: &[String],
    ) -> Result<DirectoryOutput, DirectoryError> {
        self.move_to_internal(trx, old_path, new_path).await
    }

    async fn remove(&self, trx: &Transaction, path: &[String]) -> Result<bool, DirectoryError> {
        self.remove_internal(trx, path, true).await
    }

    async fn remove_if_exists(
        &self,
        trx: &Transaction,
        path: &[String],
    ) -> Result<bool, DirectoryError> {
        self.remove_internal(trx, path, false).await
    }

    async fn list(
        &self,
        trx: &Transaction,
        path: &[String],
    ) -> Result<Vec<String>, DirectoryError> {
        self.list_internal(trx, path).await
    }
}
