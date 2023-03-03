/*
 * BlobGranuleIntegrationTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2023 Apple Inc. and the FoundationDB project authors
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

import java.util.Random;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Integration tests around Blob Granules. This requires a running FDB instance to work properly;
 * all tests will be skipped if it can't connect to a running instance relatively quickly.
 */
@ExtendWith(RequiresDatabase.class)
class BlobGranuleIntegrationTest {
    private static final FDB fdb = FDB.selectAPIVersion(ApiVersion.LATEST);

    @BeforeEach
    @AfterEach
    void clearDatabase() throws Exception {
        /*
         * Empty the database before and after each run, just in case
         */
        try (Database db = fdb.open()) {
            db.run(tr -> {
                tr.clear(Range.startsWith(new byte[] { (byte)0x00 }));
                return null;
            });
        }
    }

    @Test
    void blobManagementFunctionsTest() throws Exception {
        /*
         * A test that runs a blob range through the lifecycle of blob management.
         */
        Random rand = new Random();
        byte[] key = new byte[16];
        byte[] value = new byte[8];

        rand.nextBytes(key);
        key[0] = (byte)0x30;
        rand.nextBytes(value);

        Range blobRange = Range.startsWith(key);
        try (Database db = fdb.open()) {
            boolean blobbifySuccess = db.blobbifyRangeBlocking(blobRange.begin, blobRange.end).join();
            Assertions.assertTrue(blobbifySuccess);

            Long verifyVersion = db.verifyBlobRange(blobRange.begin, blobRange.end).join();

            Assertions.assertTrue(verifyVersion >= 0);

            // list blob ranges
            KeyRangeArrayResult blobRanges = db.listBlobbifiedRanges(blobRange.begin, blobRange.end, 2).join();
            Assertions.assertEquals(1, blobRanges.getKeyRanges().size());
            Assertions.assertArrayEquals(blobRange.begin, blobRanges.getKeyRanges().get(0).begin);
            Assertions.assertArrayEquals(blobRange.end, blobRanges.getKeyRanges().get(0).end);

            boolean flushSuccess = db.flushBlobRange(blobRange.begin, blobRange.end, false).join();
            Assertions.assertTrue(flushSuccess);

            // verify after flush
            Long verifyVersionAfterFlush = db.verifyBlobRange(blobRange.begin, blobRange.end).join();
            Assertions.assertTrue(verifyVersionAfterFlush >= 0);
            Assertions.assertTrue(verifyVersionAfterFlush >= verifyVersion);

            boolean compactSuccess = db.flushBlobRange(blobRange.begin, blobRange.end, true).join();
            Assertions.assertTrue(compactSuccess);

            Long verifyVersionAfterCompact = db.verifyBlobRange(blobRange.begin, blobRange.end).join();
            Assertions.assertTrue(verifyVersionAfterCompact >= 0);
            Assertions.assertTrue(verifyVersionAfterCompact >= verifyVersionAfterFlush);

            // purge/wait
            byte[] purgeKey = db.purgeBlobGranules(blobRange.begin, blobRange.end, -2, false).join();
            db.waitPurgeGranulesComplete(purgeKey).join();

            // verify again
            Long verifyVersionAfterPurge = db.verifyBlobRange(blobRange.begin, blobRange.end).join();
            Assertions.assertTrue(verifyVersionAfterPurge >= 0);
            Assertions.assertTrue(verifyVersionAfterPurge >= verifyVersionAfterCompact);

            // force purge/wait
            byte[] forcePurgeKey = db.purgeBlobGranules(blobRange.begin, blobRange.end, -2, true).join();
            db.waitPurgeGranulesComplete(forcePurgeKey).join();

            // check verify fails
            Long verifyVersionLast = db.verifyBlobRange(blobRange.begin, blobRange.end).join();
            Assertions.assertEquals(-1, verifyVersionLast);

            // unblobbify
            boolean unblobbifySuccess = db.unblobbifyRange(blobRange.begin, blobRange.end).join();
            Assertions.assertTrue(unblobbifySuccess);

            System.out.println("Blob granule management tests complete!");
        }
    }

    @Test
    void blobManagementFunctionsTenantTest() throws Exception {
        /*
         * A test that runs a blob range through the lifecycle of blob management.
         * Identical to the above test, but everything is scoped to a tenant instead of a database
         */
        Random rand = new Random();
        byte[] key = new byte[16];
        byte[] value = new byte[8];

        rand.nextBytes(key);
        key[0] = (byte)0x30;
        rand.nextBytes(value);

        Range blobRange = Range.startsWith(key);
        byte[] tenantName = "BGManagementTenant".getBytes();
        try (Database db = fdb.open()) {
            TenantManagement.createTenant(db, tenantName).join();

            System.out.println("Created tenant for test");

            try (Tenant tenant = db.openTenant(tenantName)) {
                System.out.println("Opened tenant for test");

                boolean blobbifySuccess = tenant.blobbifyRangeBlocking(blobRange.begin, blobRange.end).join();
                Assertions.assertTrue(blobbifySuccess);

                Long verifyVersion = tenant.verifyBlobRange(blobRange.begin, blobRange.end).join();

                Assertions.assertTrue(verifyVersion >= 0);

                // list blob ranges
                KeyRangeArrayResult blobRanges = tenant.listBlobbifiedRanges(blobRange.begin, blobRange.end, 2).join();
                Assertions.assertEquals(1, blobRanges.getKeyRanges().size());
                Assertions.assertArrayEquals(blobRange.begin, blobRanges.getKeyRanges().get(0).begin);
                Assertions.assertArrayEquals(blobRange.end, blobRanges.getKeyRanges().get(0).end);

                boolean flushSuccess = tenant.flushBlobRange(blobRange.begin, blobRange.end, false).join();
                Assertions.assertTrue(flushSuccess);

                // verify after flush
                Long verifyVersionAfterFlush = tenant.verifyBlobRange(blobRange.begin, blobRange.end).join();
                Assertions.assertTrue(verifyVersionAfterFlush >= 0);
                Assertions.assertTrue(verifyVersionAfterFlush >= verifyVersion);

                boolean compactSuccess = tenant.flushBlobRange(blobRange.begin, blobRange.end, true).join();
                Assertions.assertTrue(compactSuccess);

                Long verifyVersionAfterCompact = tenant.verifyBlobRange(blobRange.begin, blobRange.end).join();
                Assertions.assertTrue(verifyVersionAfterCompact >= 0);
                Assertions.assertTrue(verifyVersionAfterCompact >= verifyVersionAfterFlush);

                // purge/wait
                byte[] purgeKey = tenant.purgeBlobGranules(blobRange.begin, blobRange.end, -2, false).join();
                db.waitPurgeGranulesComplete(purgeKey).join();

                // verify again
                Long verifyVersionAfterPurge = tenant.verifyBlobRange(blobRange.begin, blobRange.end).join();
                Assertions.assertTrue(verifyVersionAfterPurge >= 0);
                Assertions.assertTrue(verifyVersionAfterPurge >= verifyVersionAfterCompact);

                // force purge/wait
                byte[] forcePurgeKey = tenant.purgeBlobGranules(blobRange.begin, blobRange.end, -2, true).join();
                tenant.waitPurgeGranulesComplete(forcePurgeKey).join();

                // check verify fails
                Long verifyVersionLast = tenant.verifyBlobRange(blobRange.begin, blobRange.end).join();
                Assertions.assertEquals(-1, verifyVersionLast);

                // unblobbify
                boolean unblobbifySuccess = tenant.unblobbifyRange(blobRange.begin, blobRange.end).join();
                Assertions.assertTrue(unblobbifySuccess);

                System.out.println("Blob granule management tenant tests complete!");
            }
        }
    }
}
