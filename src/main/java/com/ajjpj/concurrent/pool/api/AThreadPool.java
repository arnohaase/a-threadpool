package com.ajjpj.concurrent.pool.api;


public interface AThreadPool {
    void submit (Runnable code);

    AThreadPool SYNC_THREADPOOL = new AThreadPool () {
        @Override public void submit (Runnable code) {
            code.run ();
        }
    };
}
