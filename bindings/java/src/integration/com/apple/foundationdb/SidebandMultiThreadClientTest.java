package com.apple.foundationdb;

import com.apple.foundationdb.tuple.Tuple;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.jupiter.api.Assertions;

/**
 * Each cluster has a queue, producer writes a key and then send a message to this queue in JVM.
 * Consumer would consume the key by checking the existence of the key, if it does not find the key,
 * then the test would fail.
 *
 * This test is to verify the causal consistency of transactions for mutli-threaded client. 
 */
public class SidebandMultiThreadClientTest {
    public static final MultiClientHelper clientHelper = new MultiClientHelper();

    private static final Map<Database, BlockingQueue<String>> db2Queues = new HashMap<>();
    private static final int threadPerDB = 5;
    private static final int txnCnt = 1000;

    public static void main(String[] args) throws Exception {
        FDB fdb = FDB.selectAPIVersion(720);
        setupThreads(fdb);
        Collection<Database> dbs = clientHelper.openDatabases(fdb); // the clientHelper will close the databases for us
        for (Database db : dbs) {
            db2Queues.put(db, new LinkedBlockingQueue<>());
        }
        System.out.println("Start processing and validating");
        process(dbs);
        check(dbs);
        System.out.println("Test finished");
    }

    private static synchronized void setupThreads(FDB fdb) {
        int clientThreadsPerVersion = clientHelper.readClusterFromEnv().length;
        fdb.options().setClientThreadsPerVersion(clientThreadsPerVersion);
        System.out.printf("thread per version is %d\n", clientThreadsPerVersion);
        fdb.options().setExternalClientDirectory("/var/dynamic-conf/lib");
        fdb.options().setTraceEnable("/tmp");
        fdb.options().setKnob("min_trace_severity=5");
    }

    private static void process(Collection<Database> dbs) {
        for (Database db : dbs) {
            for (int i = 0; i < threadPerDB; i++) {
                final Thread thread = new Thread(Producer.create(db, db2Queues.get(db)));
                thread.start();
            }
        }
    }

    private static void check(Collection<Database> dbs) throws InterruptedException {
        final Map<Thread, Consumer> threads2Consumers = new HashMap<>();
        for (Database db : dbs) {
            for (int i = 0; i < threadPerDB; i++) {
                final Consumer consumer = Consumer.create(db, db2Queues.get(db));
                final Thread thread = new Thread(consumer);
                thread.start();
                threads2Consumers.put(thread, consumer);
            }
        }

        for (Map.Entry<Thread, Consumer> entry : threads2Consumers.entrySet()) {
            entry.getKey().join();
            final boolean succeed = entry.getValue().succeed;
            Assertions.assertTrue(succeed, "Sideband test failed");
        }
    }

    public static class Producer implements Runnable {
        private final Database db;
        private final BlockingQueue<String> queue;

        private Producer(Database db, BlockingQueue<String> queue) {
            this.db = db;
            this.queue = queue;
        }

        public static Producer create(Database db, BlockingQueue<String> queue) {
            return new Producer(db, queue);
        }

        @Override
        public void run() {
            for (int i = 0; i < txnCnt; i++) {
                final long suffix = ThreadLocalRandom.current().nextLong();
                final String key = String.format("Sideband/Multithread/Test/%d", suffix);
                db.run(tr -> {
                    tr.set(Tuple.from(key).pack(), Tuple.from("bar").pack());
                    return null;
                });
                queue.offer(key);
            }
        }
    }

    public static class Consumer implements Runnable {
        private final Database db;
        private final BlockingQueue<String> queue;
        private boolean succeed;

        private Consumer(Database db, BlockingQueue<String> queue) {
            this.db = db;
            this.queue = queue;
            this.succeed = true;
        }

        public static Consumer create(Database db, BlockingQueue<String> queue) {
            return new Consumer(db, queue);
        }

        @Override
        public void run() {
            try {
                for (int i = 0; i < txnCnt && succeed; i++) {
                    final String key = queue.take();
                    db.run(tr -> {
                        byte[] result = tr.get(Tuple.from(key).pack()).join();
                        if (result == null) {
                            System.out.println("FAILED to get key " + key + " from DB " + db);
                            succeed = false;
                        }
                        if (!succeed) {
                            return null;
                        }
                        String value = Tuple.fromBytes(result).getString(0);
                        return null;
                    });
                }
            } catch (InterruptedException e) {
                System.out.println("Get Exception in consumer: " + e);
                succeed = false;
            }
        }
    }
}
