// Copyright 2018 foundationdb-rs developers, https://github.com/Clikengo/foundationdb-rs/graphs/contributors
// Copyright 2013-2018 Apple, Inc and the FoundationDB project authors.
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

//! A resulting Subspace whose prefix is preprended to all of its descendant directories's prefixes.

use crate::Transaction;
use crate::directory::directory_layer::{DEFAULT_NODE_PREFIX, DirectoryLayer, PARTITION_LAYER};
use crate::directory::directory_subspace::DirectorySubspace;
use crate::directory::error::DirectoryError;
use crate::directory::{Directory, DirectoryOutput};
use crate::tuple::Subspace;
use async_trait::async_trait;
use std::ops::Deref;
use std::sync::Arc;

/// A `DirectoryPartition` is a DirectorySubspace whose prefix is preprended to all of its descendant
/// directories's prefixes. It cannot be used as a Subspace. Instead, you must create at
/// least one subdirectory to store content.
#[derive(Clone)]
pub struct DirectoryPartition {
    pub(crate) inner: Arc<DirectoryPartitionInner>,
}

#[derive(Debug)]
pub struct DirectoryPartitionInner {
    pub(crate) directory_subspace: DirectorySubspace,
    pub(crate) parent_directory_layer: DirectoryLayer,
}

impl Deref for DirectoryPartition {
    type Target = DirectoryPartitionInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl std::fmt::Debug for DirectoryPartition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

impl DirectoryPartition {
    // https://github.com/apple/foundationdb/blob/master/bindings/flow/DirectoryPartition.h#L34-L43
    pub(crate) fn new(
        path: &[String],
        prefix: Vec<u8>,
        parent_directory_layer: DirectoryLayer,
    ) -> Self {
        let mut node_subspace_bytes = vec![];
        node_subspace_bytes.extend_from_slice(&prefix);
        node_subspace_bytes.extend_from_slice(DEFAULT_NODE_PREFIX);

        let new_directory_layer = DirectoryLayer::new_with_path(
            Subspace::from_bytes(node_subspace_bytes),
            Subspace::from_bytes(prefix.as_slice()),
            false,
            path,
        );

        DirectoryPartition {
            inner: Arc::new(DirectoryPartitionInner {
                directory_subspace: DirectorySubspace::new(
                    path,
                    prefix,
                    &new_directory_layer,
                    Vec::from(PARTITION_LAYER),
                ),
                parent_directory_layer,
            }),
        }
    }
}

impl DirectoryPartition {
    pub fn get_path(&self) -> &[String] {
        self.inner.directory_subspace.get_path()
    }

    fn get_directory_layer_for_path(&self, path: &[String]) -> DirectoryLayer {
        if path.is_empty() {
            self.parent_directory_layer.clone()
        } else {
            self.directory_subspace.directory_layer.clone()
        }
    }

    fn get_partition_subpath(
        &self,
        path: &[String],
        directory_layer: Option<DirectoryLayer>,
    ) -> Result<Vec<String>, DirectoryError> {
        let directory = match directory_layer {
            None => self.directory_subspace.directory_layer.clone(),
            Some(d) => d,
        };

        if directory.path.len() > self.directory_subspace.get_path().len() {
            return Err(DirectoryError::CannotCreateSubpath);
        }

        let mut new_path = vec![];

        new_path.extend_from_slice(&self.directory_subspace.get_path()[directory.path.len()..]);
        new_path.extend_from_slice(path);

        Ok(new_path)
    }

    pub fn get_layer(&self) -> &[u8] {
        b"partition"
    }
}

#[async_trait]
impl Directory for DirectoryPartition {
    async fn create_or_open(
        &self,
        txn: &Transaction,
        path: &[String],
        prefix: Option<&[u8]>,
        layer: Option<&[u8]>,
    ) -> Result<DirectoryOutput, DirectoryError> {
        self.inner
            .directory_subspace
            .create_or_open(txn, path, prefix, layer)
            .await
    }

    async fn create(
        &self,
        txn: &Transaction,
        path: &[String],
        prefix: Option<&[u8]>,
        layer: Option<&[u8]>,
    ) -> Result<DirectoryOutput, DirectoryError> {
        self.inner
            .directory_subspace
            .create(txn, path, prefix, layer)
            .await
    }

    async fn open(
        &self,
        txn: &Transaction,
        path: &[String],
        layer: Option<&[u8]>,
    ) -> Result<DirectoryOutput, DirectoryError> {
        self.inner.directory_subspace.open(txn, path, layer).await
    }

    async fn exists(&self, trx: &Transaction, path: &[String]) -> Result<bool, DirectoryError> {
        let directory_layer = self.get_directory_layer_for_path(path);

        directory_layer
            .exists(
                trx,
                &self.get_partition_subpath(path, Some(directory_layer.clone()))?,
            )
            .await
    }

    async fn move_directory(
        &self,
        trx: &Transaction,
        new_path: &[String],
    ) -> Result<DirectoryOutput, DirectoryError> {
        let directory_layer = self.get_directory_layer_for_path(&[]);
        let directory_layer_path = &directory_layer.path;

        if directory_layer_path.len() > new_path.len() {
            return Err(DirectoryError::CannotMoveBetweenPartition);
        }

        for (i, path) in directory_layer_path.iter().enumerate() {
            match new_path.get(i) {
                None => return Err(DirectoryError::CannotMoveBetweenPartition),
                Some(new_path_item) => {
                    if !new_path_item.eq(path) {
                        return Err(DirectoryError::CannotMoveBetweenPartition);
                    }
                }
            }
        }

        let mut new_relative_path = vec![];
        new_relative_path.extend_from_slice(&new_path[directory_layer_path.len()..]);

        directory_layer
            .move_to(
                trx,
                &self.get_partition_subpath(&[], Some(directory_layer.clone()))?,
                &new_relative_path,
            )
            .await
    }

    async fn move_to(
        &self,
        trx: &Transaction,
        old_path: &[String],
        new_path: &[String],
    ) -> Result<DirectoryOutput, DirectoryError> {
        self.inner
            .directory_subspace
            .move_to(trx, old_path, new_path)
            .await
    }

    async fn remove(&self, trx: &Transaction, path: &[String]) -> Result<bool, DirectoryError> {
        let directory_layer = self.get_directory_layer_for_path(path);
        directory_layer
            .remove(
                trx,
                &self.get_partition_subpath(path, Some(directory_layer.clone()))?,
            )
            .await
    }

    async fn remove_if_exists(
        &self,
        trx: &Transaction,
        path: &[String],
    ) -> Result<bool, DirectoryError> {
        let directory_layer = self.get_directory_layer_for_path(path);
        directory_layer
            .remove_if_exists(
                trx,
                &self.get_partition_subpath(path, Some(directory_layer.clone()))?,
            )
            .await
    }

    async fn list(
        &self,
        trx: &Transaction,
        path: &[String],
    ) -> Result<Vec<String>, DirectoryError> {
        self.inner.directory_subspace.list(trx, path).await
    }
}
