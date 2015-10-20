package com.ajjpj.concurrent.pool.b;

import benchmark.PoolBenchmark;

import java.util.concurrent.Callable;


/**
 * @author arno
 */
public class AThreadPoolTask<T> {
    final Callable<T> code;

    //TODO clean up this hack
    public final PoolBenchmark.SettableFutureTask<T> future = new PoolBenchmark.SettableFutureTask<T> (null);

    public AThreadPoolTask (Callable<T> code) {
        this.code = code;
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
