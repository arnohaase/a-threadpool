package com.ajjpj.concurrent.pool.api;

import java.util.Collections;
import java.util.List;


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

        @Override public State getState () {
            return isShutdown ? State.Down : State.Rrunning;
        }

        @Override public List<AFuture<Void>> shutdown (ShutdownMode shutdownMode) {
            isShutdown = true;
            return Collections.emptyList ();
        }

    };
}
