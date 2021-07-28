package com.apple.foundationdb;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Default executor for various call sites when an executor is needed but unspecified.
 */
public abstract class DefaultExecutor {

    static class DaemonThreadFactory implements ThreadFactory {
        private final ThreadFactory factory;
        private static AtomicInteger threadCount = new AtomicInteger();

        DaemonThreadFactory(ThreadFactory factory) {
            this.factory = factory;
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread t = factory.newThread(r);
            t.setName("fdb-java-" + threadCount.incrementAndGet());
            t.setDaemon(true);
            return t;
        }
    }

    public static final ExecutorService DEFAULT_EXECUTOR;

    static {
        ThreadFactory factory = new DaemonThreadFactory(Executors.defaultThreadFactory());
        DEFAULT_EXECUTOR = Executors.newCachedThreadPool(factory);
    }
}
