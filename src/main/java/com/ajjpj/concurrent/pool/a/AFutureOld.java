package com.ajjpj.concurrent.pool.a;

import java.util.concurrent.ExecutionException;


/**
 * @author arno
 */
public interface AFutureOld<T> {
    boolean isDone ();
    T get () throws InterruptedException, ExecutionException;
}
