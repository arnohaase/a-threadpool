package com.ajjpj.concurrent.pool.impl;

import com.ajjpj.concurrent.pool.api.ASettableFuture;


/**
 * This Throwable is thrown by a special 'shutdown' task to signal worker threads to
 *  shut down without burdening regular execution.
 *
 * @author arno
 */
class PoolShutdown extends Error {
    final ASettableFuture<Void> shutdownFuture;

    PoolShutdown (ASettableFuture<Void> shutdownFuture) {
        this.shutdownFuture = shutdownFuture;
    }

    @Override public Throwable fillInStackTrace () {
        return this;
    }
}
