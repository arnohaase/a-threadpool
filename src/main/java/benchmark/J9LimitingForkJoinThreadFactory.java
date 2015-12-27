package benchmark;


import jdk.j9new.ForkJoinPool;
import jdk.j9new.ForkJoinWorkerThread;

import java.util.concurrent.atomic.AtomicInteger;

public class J9LimitingForkJoinThreadFactory implements ForkJoinPool.ForkJoinWorkerThreadFactory {
    final int totalNumThreads;
    final AtomicInteger created = new AtomicInteger(0);

    public J9LimitingForkJoinThreadFactory(int totalNumThreads) {
        this.totalNumThreads = totalNumThreads;
    }

    @Override public ForkJoinWorkerThread newThread(ForkJoinPool pool) {
        if (created.getAndIncrement() < totalNumThreads)
            return new FJThread(pool);
        return null;
    }

    static class FJThread extends ForkJoinWorkerThread {
        public FJThread(ForkJoinPool pool) {
            super(pool);
        }
    }
}
