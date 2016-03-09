package com.ajjpj.concurrent.pool.api;


public interface AThreadPool {
    void submit (Runnable code);

    AThreadPoolWithAdmin SYNC_THREADPOOL = new AThreadPoolWithAdmin () {
        private volatile boolean isShutdown = false;

        @Override public void submit (Runnable code) {
            code.run ();
        }

        @Override public AThreadPoolStatistics getStatistics () {
            return AThreadPoolStatistics.NONE;
        }

        @Override public boolean isShutdown () {
            return isShutdown;
        }

        @Override public void shutdown () {
            isShutdown = true;
        }
    };
}
