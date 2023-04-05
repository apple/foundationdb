/*
 * TenantManagement.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.apple.foundationdb;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;

import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.CloseableAsyncIterator;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;

/**
 * The FoundationDB API includes function to manage the set of tenants in a cluster.
 */
public class TenantManagement {
	static byte[] TENANT_MAP_PREFIX = ByteArrayUtil.join(new byte[] { (byte)255, (byte)255 },
	                                                     "/management/tenant/map/".getBytes());

	/**
	 * Creates a new tenant in the cluster. If the tenant already exists, this operation will complete
	 * successfully without changing anything. The transaction must be committed for the creation to take
	 * effect or to observe any errors.
	 *
	 * @param tr The transaction used to create the tenant.
	 * @param tenantName The name of the tenant. Can be any byte string that does not begin a 0xFF byte.
	 */
	public static void createTenant(Transaction tr, byte[] tenantName) {
		tr.options().setSpecialKeySpaceEnableWrites();
		tr.set(ByteArrayUtil.join(TENANT_MAP_PREFIX, tenantName), new byte[0]);
	}

	/**
	 * Creates a new tenant in the cluster. If the tenant already exists, this operation will complete
	 * successfully without changing anything. The transaction must be committed for the creation to take
	 * effect or to observe any errors.<br>
	 * <br>
	 * This is a convenience method that generates the tenant name by packing a {@code Tuple}.
	 *
	 * @param tr The transaction used to create the tenant.
	 * @param tenantName The name of the tenant, as a Tuple.
	 */
	public static void createTenant(Transaction tr, Tuple tenantName) {
		createTenant(tr, tenantName.pack());
	}

	/**
	 * Creates a new tenant in the cluster using a transaction created on the specified {@code Database}.
	 * This operation will first check whether the tenant exists, and if it does it will set the
	 * {@code CompletableFuture} to a tenant_already_exists error. Otherwise, it will attempt to create
	 * the tenant in a retry loop. If the tenant is created concurrently by another transaction, this
	 * function may still return successfully.
	 *
	 * @param db The database used to create a transaction for creating the tenant.
	 * @param tenantName The name of the tenant. Can be any byte string that does not begin a 0xFF byte.
	 * @return a {@code CompletableFuture} that when set without error will indicate that the tenant has
	 * been created.
	 */
	public static CompletableFuture<Void> createTenant(Database db, byte[] tenantName) {
		final AtomicBoolean checkedExistence = new AtomicBoolean(false);
		final byte[] key = ByteArrayUtil.join(TENANT_MAP_PREFIX, tenantName);
		return db.runAsync(tr -> {
			tr.options().setSpecialKeySpaceEnableWrites();
			if(checkedExistence.get()) {
				tr.set(key, new byte[0]);
				return CompletableFuture.completedFuture(null);
			}
			else {
				return tr.get(key).thenAcceptAsync(result -> {
					checkedExistence.set(true);
					if(result != null) {
						throw new FDBException("A tenant with the given name already exists", 2132);
					}
					tr.set(key, new byte[0]);
				});
			}
		});
	}

	/**
	 * Creates a new tenant in the cluster using a transaction created on the specified {@code Database}.
	 * This operation will first check whether the tenant exists, and if it does it will set the
	 * {@code CompletableFuture} to a tenant_already_exists error. Otherwise, it will attempt to create
	 * the tenant in a retry loop. If the tenant is created concurrently by another transaction, this
	 * function may still return successfully.<br>
	 * <br>
	 * This is a convenience method that generates the tenant name by packing a {@code Tuple}.
	 *
	 * @param db The database used to create a transaction for creating the tenant.
	 * @param tenantName The name of the tenant, as a Tuple.
	 * @return a {@code CompletableFuture} that when set without error will indicate that the tenant has
	 * been created.
	 */
	public static CompletableFuture<Void> createTenant(Database db, Tuple tenantName) {
		return createTenant(db, tenantName.pack());
	}

	/**
	 * Deletes a tenant from the cluster. If the tenant does not exists, this operation will complete
	 * successfully without changing anything. The transaction must be committed for the deletion to take
	 * effect or to observe any errors.<br>
	 * <br>
	 * <b>Note:</b> A tenant cannot be deleted if it has any data in it. To delete a non-empty tenant, you must
	 * first use a clear operation to delete all of its keys.
	 *
	 * @param tr The transaction used to delete the tenant.
	 * @param tenantName The name of the tenant being deleted.
	 */
	public static void deleteTenant(Transaction tr, byte[] tenantName) {
		tr.options().setSpecialKeySpaceEnableWrites();
		tr.clear(ByteArrayUtil.join(TENANT_MAP_PREFIX, tenantName));
	}

	/**
	 * Deletes a tenant from the cluster. If the tenant does not exists, this operation will complete
	 * successfully without changing anything. The transaction must be committed for the deletion to take
	 * effect or to observe any errors.<br>
	 * <br>
	 * <b>Note:</b> A tenant cannot be deleted if it has any data in it. To delete a non-empty tenant, you must
	 * first use a clear operation to delete all of its keys.<br>
	 * <br>
	 * This is a convenience method that generates the tenant name by packing a {@code Tuple}.
	 *
	 * @param tr The transaction used to delete the tenant.
	 * @param tenantName The name of the tenant being deleted, as a Tuple.
	 */
	public static void deleteTenant(Transaction tr, Tuple tenantName) {
		deleteTenant(tr, tenantName.pack());
	}

	/**
	 * Deletes a tenant from the cluster using a transaction created on the specified {@code Database}. This
	 * operation will first check whether the tenant exists, and if it does not it will set the
	 * {@code CompletableFuture} to a tenant_not_found error. Otherwise, it will attempt to delete the
	 * tenant in a retry loop. If the tenant is deleted concurrently by another transaction, this function may
	 * still return successfully.<br>
	 * <br>
	 * <b>Note:</b> A tenant cannot be deleted if it has any data in it. To delete a non-empty tenant, you must
	 * first use a clear operation to delete all of its keys.
	 *
	 * @param db The database used to create a transaction for deleting the tenant.
	 * @param tenantName The name of the tenant being deleted.
	 * @return a {@code CompletableFuture} that when set without error will indicate that the tenant has
	 * been deleted.
	 */
	public static CompletableFuture<Void> deleteTenant(Database db, byte[] tenantName) {
		final AtomicBoolean checkedExistence = new AtomicBoolean(false);
		final byte[] key = ByteArrayUtil.join(TENANT_MAP_PREFIX, tenantName);
		return db.runAsync(tr -> {
			tr.options().setSpecialKeySpaceEnableWrites();
			if(checkedExistence.get()) {
				tr.clear(key);
				return CompletableFuture.completedFuture(null);
			}
			else {
				return tr.get(key).thenAcceptAsync(result -> {
					checkedExistence.set(true);
					if(result == null) {
						throw new FDBException("Tenant does not exist", 2131);
					}
					tr.clear(key);
				});
			}
		});
	}

	/**
	 * Deletes a tenant from the cluster using a transaction created on the specified {@code Database}. This
	 * operation will first check whether the tenant exists, and if it does not it will set the
	 * {@code CompletableFuture} to a tenant_not_found error. Otherwise, it will attempt to delete the
	 * tenant in a retry loop. If the tenant is deleted concurrently by another transaction, this function may
	 * still return successfully.<br>
	 * <br>
	 * <b>Note:</b> A tenant cannot be deleted if it has any data in it. To delete a non-empty tenant, you must
	 * first use a clear operation to delete all of its keys.<br>
	 * <br>
	 * This is a convenience method that generates the tenant name by packing a {@code Tuple}.
	 *
	 * @param db The database used to create a transaction for deleting the tenant.
	 * @param tenantName The name of the tenant being deleted.
	 * @return a {@code CompletableFuture} that when set without error will indicate that the tenant has
	 * been deleted.
	 */
	public static CompletableFuture<Void> deleteTenant(Database db, Tuple tenantName) {
		return deleteTenant(db, tenantName.pack());
	}


	/**
	 * Lists all tenants in between the range specified. The number of tenants listed can be restricted.
	 *
	 * @param db The database used to create a transaction for listing the tenants.
	 * @param begin The beginning of the range of tenants to list.
	 * @param end The end of the range of the tenants to list.
	 * @param limit The maximum number of tenants to return from this request.
	 * @return an iterator where each item is a KeyValue object where the key is the tenant name
	 * and the value is the unprocessed JSON string containing the tenant's metadata
	 */
	public static CloseableAsyncIterator<KeyValue> listTenants(Database db, byte[] begin, byte[] end, int limit) {
		return listTenants_internal(db.createTransaction(), begin, end, limit);
	}

	/**
	 * Lists all tenants in between the range specified. The number of tenants listed can be restricted.
	 * This is a convenience method that generates the begin and end ranges by packing two {@code Tuple}s.
	 *
	 * @param db The database used to create a transaction for listing the tenants.
	 * @param begin The beginning of the range of tenants to list.
	 * @param end The end of the range of the tenants to list.
	 * @param limit The maximum number of tenants to return from this request.
	 * @return an iterator where each item is a KeyValue object where the key is the tenant name
	 * and the value is the unprocessed JSON string containing the tenant's metadata
	 */
	public static CloseableAsyncIterator<KeyValue> listTenants(Database db, Tuple begin, Tuple end, int limit) {
		return listTenants_internal(db.createTransaction(), begin.pack(), end.pack(), limit);
	}

	private static CloseableAsyncIterator<KeyValue> listTenants_internal(Transaction tr, byte[] begin, byte[] end,
	                                                                   int limit) {
		return new TenantAsyncIterator(tr, begin, end, limit);
	}

	// Templates taken from BoundaryIterator LocalityUtil.java
	static class TenantAsyncIterator implements CloseableAsyncIterator<KeyValue> {
		Transaction tr;
		final byte[] begin;
		final byte[] end;

		final AsyncIterable<KeyValue> firstGet;
		AsyncIterator<KeyValue> iter;
		private boolean closed;

		TenantAsyncIterator(Transaction tr, byte[] begin, byte[] end, int limit) {
			this.tr = tr;

			this.begin = ByteArrayUtil.join(TENANT_MAP_PREFIX, begin);
			this.end = ByteArrayUtil.join(TENANT_MAP_PREFIX, end);

			tr.options().setRawAccess();
			tr.options().setLockAware();

			firstGet = tr.getRange(this.begin, this.end, limit);
			iter = firstGet.iterator();
			closed = false;
		}

		@Override
		public CompletableFuture<Boolean> onHasNext() {
			return iter.onHasNext();
		}

		@Override
		public boolean hasNext() {
			return iter.hasNext();
		}
		@Override
		public KeyValue next() {
			KeyValue kv = iter.next();
			byte[] tenant = Arrays.copyOfRange(kv.getKey(), TENANT_MAP_PREFIX.length, kv.getKey().length);
			byte[] value = kv.getValue();

			KeyValue result = new KeyValue(tenant, value);
			return result;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException("Tenant lists are read-only");
		}

		@Override
		public void close() {
			TenantAsyncIterator.this.tr.close();
			closed = true;
		}

		@Override
		protected void finalize() throws Throwable {
			try {
				if (FDB.instance().warnOnUnclosed && !closed) {
					System.err.println("CloseableAsyncIterator not closed (listTenants)");
				}
				if (!closed) {
					close();
				}
			} finally {
				super.finalize();
			}
		}
	}

	private TenantManagement() {}
}
