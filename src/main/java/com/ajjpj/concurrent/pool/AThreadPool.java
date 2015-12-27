package com.ajjpj.concurrent.pool;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;


public interface AThreadPool {
    <T> Future<T> submit (Callable<T> code);

    void shutdown();
    void awaitTermination() throws InterruptedException;

    AThreadPoolStatistics getStatistics();
}
