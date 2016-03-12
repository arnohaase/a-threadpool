package com.ajjpj.concurrent.pool.api;

import java.util.concurrent.Executor;


public interface AThreadPool {
    void submit (Runnable task);

    static AThreadPool wrap (Executor es) {
        return new AThreadPool () {
            @Override public void submit (Runnable task) {
                es.execute (task);
            }
        };
    }

    AThreadPool SYNC_THREADPOOL = new AThreadPool () {
        @Override public void submit (Runnable code) {
            code.run ();
        }
    };
}
