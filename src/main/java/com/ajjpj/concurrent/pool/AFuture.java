package com.ajjpj.concurrent.pool;

import java.util.concurrent.ExecutionException;


/**
 * @author arno
 */
public interface AFuture<T> {
    boolean isDone ();
    T get () throws InterruptedException, ExecutionException;
}
