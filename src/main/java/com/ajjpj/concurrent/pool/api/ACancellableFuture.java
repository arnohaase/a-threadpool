package com.ajjpj.concurrent.pool.api;

import com.ajjpj.afoundation.function.AFunction0;

import java.util.concurrent.Callable;


public interface ACancellableFuture<T> extends AFuture<T> {
    default boolean cancel () {
        return cancel (false);
    }
    boolean cancel (boolean interruptRunning);

    boolean isCancelled ();

    //TODO wrap the Callable or AFunction0 in something that can handle 'cancel'

    static <T> ACancellableFuture<T> submit (AThreadPool tp, Callable<T> f) {
        return null; //TODO
    }

    static <T> ACancellableFuture<T> submit (AThreadPool tp, AFunction0<T,?> f) {
        return null; //TODO
    }
}
