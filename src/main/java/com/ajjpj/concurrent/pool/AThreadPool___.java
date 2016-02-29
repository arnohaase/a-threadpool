package com.ajjpj.concurrent.pool;

import com.ajjpj.concurrent.pool.api.AThreadPoolStatistics;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;


public interface AThreadPool___ {
    <T> Future<T> submit (Callable<T> code);

    void shutdown();
    void awaitTermination() throws InterruptedException;

    AThreadPoolStatistics getStatistics();
}
