package com.ajjpj.concurrent.pool.api;

import com.ajjpj.concurrent.pool.api.other.ATry;


public interface ASettableFuture<T> extends AFuture<T> {
    static <T> ASettableFuture<T> create () {
        return create (AThreadPool.SYNC_THREADPOOL);
    }

    static <T> ASettableFuture<T> create (AThreadPool pool) {
        return new AFutureImpl<> (pool);
    }

    void complete (ATry<T> o);
    boolean tryComplete (ATry<T> o);

    default void completeAsSuccess (T o) {
        complete (ATry.success (o));
    }

    default void completeAsFailure (Throwable th) {
        complete (ATry.failure (th));
    }
}
