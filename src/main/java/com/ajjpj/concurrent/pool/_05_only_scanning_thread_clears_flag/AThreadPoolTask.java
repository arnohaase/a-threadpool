package com.ajjpj.concurrent.pool._05_only_scanning_thread_clears_flag;

import benchmark.PoolBenchmark;

import java.util.concurrent.Callable;


/**
 * @author arno
 */
public class AThreadPoolTask<T> {
    final Callable<T> code;

    //TODO clean up this hack
    public final PoolBenchmark.SettableFutureTask<T> future;

    public AThreadPoolTask (Callable<T> code) {
        this.code = code;
        this.future = new PoolBenchmark.SettableFutureTask<T> (code);
    }

    void execute() {
        try {
            future.set (code.call ());
        }
        catch (Exception e) {
            future.setException (e);
            e.printStackTrace ();
        }
    }
}
