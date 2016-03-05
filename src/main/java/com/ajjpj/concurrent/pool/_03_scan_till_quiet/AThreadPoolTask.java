package com.ajjpj.concurrent.pool._03_scan_till_quiet;

import benchmark.SettableFutureTask;

import java.util.concurrent.Callable;


/**
 * @author arno
 */
public class AThreadPoolTask<T> {
    final Callable<T> code;

    //TODO clean up this hack
    public final SettableFutureTask<T> future;

    public AThreadPoolTask (Callable<T> code) {
        this.code = code;
        this.future = new SettableFutureTask<T> (code);
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
