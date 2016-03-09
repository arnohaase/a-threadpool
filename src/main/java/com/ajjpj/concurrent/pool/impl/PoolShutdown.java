package com.ajjpj.concurrent.pool.impl;

import com.ajjpj.concurrent.pool.api.AFutureImpl;
import com.ajjpj.concurrent.pool.api.AThreadPool;


/**
 * This Throwable is thrown by a special 'shutdown' task to signal worker threads to
 *  shut down without burdening regular execution.
 *
 * @author arno
 */
class PoolShutdown extends Error {
    final AFutureImpl<Void> shutdownFuture = new AFutureImpl<> (AThreadPool.SYNC_THREADPOOL); //TODO ASettableFuture

    @Override public Throwable fillInStackTrace () {
        return this;
    }
}
