// Copyright 2018 foundationdb-rs developers, https://github.com/Clikengo/foundationdb-rs/graphs/contributors
// Copyright 2013-2018 Apple, Inc and the FoundationDB project authors.
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

//! The resulting Subspace generated with a Directory

use crate::Transaction;
use crate::directory::directory_layer::DirectoryLayer;
use crate::directory::error::DirectoryError;
use crate::directory::{Directory, DirectoryOutput};
use crate::tuple::{PackResult, Subspace, TuplePack, TupleUnpack};
use async_trait::async_trait;

/// A `DirectorySubspace` represents the contents of a directory, but it also remembers
/// the path with which it was opened and offers convenience methods to operate on the directory at that path.
/// An instance of `DirectorySubspace` can be used for all the usual subspace operations.
/// It can also be used to operate on the directory with which it was opened.
#[derive(Debug, Clone)]
pub struct DirectorySubspace {
    pub(crate) directory_layer: DirectoryLayer,
    subspace: Subspace,
    path: Vec<String>,
    layer: Vec<u8>,
}

impl DirectorySubspace {
    pub fn new(
        path: &[String],
        prefix: Vec<u8>,
        directory_layer: &DirectoryLayer,
        layer: Vec<u8>,
    ) -> Self {
        DirectorySubspace {
            directory_layer: directory_layer.clone(),
            subspace: Subspace::from_bytes(prefix),
            path: Vec::from(path),
            layer,
        }
    }

    // https://github.com/apple/foundationdb/blob/master/bindings/flow/DirectorySubspace.cpp#L105
    fn get_partition_subpath(
        &self,
        path: &[String],
        directory_layer: Option<DirectoryLayer>,
    ) -> Result<Vec<String>, DirectoryError> {
        let directory = match directory_layer {
            None => self.directory_layer.clone(),
            Some(d) => d,
        };

        if directory.path.len() > self.path.len() {
            return Err(DirectoryError::CannotCreateSubpath);
        }

        let mut new_path = vec![];

        new_path.extend_from_slice(&self.path[directory.path.len()..]);
        new_path.extend_from_slice(path);

        Ok(new_path)
    }
}

impl DirectorySubspace {
    pub fn subspace<T: TuplePack>(&self, t: &T) -> Subspace {
        self.subspace.subspace(t)
    }

    pub fn bytes(&self) -> &[u8] {
        self.subspace.bytes()
    }

    pub fn pack<T: TuplePack>(&self, t: &T) -> Vec<u8> {
        self.subspace.pack(t)
    }

    pub fn unpack<'de, T: TupleUnpack<'de>>(&self, key: &'de [u8]) -> PackResult<T> {
        self.subspace.unpack(key)
    }

    pub fn range(&self) -> (Vec<u8>, Vec<u8>) {
        self.subspace.range()
    }

    pub fn get_path(&self) -> &[String] {
        self.path.as_slice()
    }

    pub fn set_path(&mut self, path: Vec<String>) {
        self.path = path;
    }

    pub fn get_layer(&self) -> &[u8] {
        self.layer.as_slice()
    }

    pub fn is_start_of(&self, key: &[u8]) -> bool {
        self.subspace.is_start_of(key)
    }

    fn get_directory_layer_for_path(&self, _: &[String]) -> DirectoryLayer {
        self.directory_layer.clone()
    }
}

#[async_trait]
impl Directory for DirectorySubspace {
    async fn create_or_open(
        &self,
        txn: &Transaction,
        path: &[String],
        prefix: Option<&[u8]>,
        layer: Option<&[u8]>,
    ) -> Result<DirectoryOutput, DirectoryError> {
        self.directory_layer
            .create_or_open(txn, &self.get_partition_subpath(path, None)?, prefix, layer)
            .await
    }

    async fn create(
        &self,
        txn: &Transaction,
        path: &[String],
        prefix: Option<&[u8]>,
        layer: Option<&[u8]>,
    ) -> Result<DirectoryOutput, DirectoryError> {
        self.directory_layer
            .create(txn, &self.get_partition_subpath(path, None)?, prefix, layer)
            .await
    }

    async fn open(
        &self,
        txn: &Transaction,
        path: &[String],
        layer: Option<&[u8]>,
    ) -> Result<DirectoryOutput, DirectoryError> {
        self.directory_layer
            .open(txn, &self.get_partition_subpath(path, None)?, layer)
            .await
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
        self.directory_layer
            .move_to(
                trx,
                &self.get_partition_subpath(old_path, None)?,
                &self.get_partition_subpath(new_path, None)?,
            )
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
        self.directory_layer
            .list(trx, &self.get_partition_subpath(path, None)?)
            .await
    }
}
