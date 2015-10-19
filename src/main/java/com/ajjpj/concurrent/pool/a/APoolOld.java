package com.ajjpj.concurrent.pool.a;

import java.util.concurrent.Callable;


/**
 * @author arno
 */
public interface APoolOld {
    <T> AFutureOld<T> submit (Callable<T> code);
    void shutdown () throws InterruptedException;
}
