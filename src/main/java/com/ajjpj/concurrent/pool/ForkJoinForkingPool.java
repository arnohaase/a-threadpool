package com.ajjpj.concurrent.pool;

import java.util.concurrent.*;


/**
 * @author arno
 */
public class ForkJoinForkingPool implements APool {
    private final ForkJoinPool ec;

    public ForkJoinForkingPool (ForkJoinPool ec) {
        this.ec = ec;
    }

    @Override public <T> AFuture<T> submit (Callable<T> code) {
        if (Thread.currentThread () instanceof ForkJoinWorkerThread) {
            final ForkJoinTask<T> task = ForkJoinTask.adapt (code);
            task.fork ();
            return new WrappingAFuture<> (task);
        }

        return new WrappingAFuture<> (ec.submit (code));
    }

    @Override public void shutdown () throws InterruptedException {
        ec.shutdown ();
    }
}
