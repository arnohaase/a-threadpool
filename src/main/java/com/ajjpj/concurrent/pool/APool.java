package com.ajjpj.concurrent.pool;

import java.util.concurrent.Callable;


/**
 * @author arno
 */
public interface APool {
    <T> AFuture<T> submit (Callable<T> code);
    void shutdown () throws InterruptedException;
}
