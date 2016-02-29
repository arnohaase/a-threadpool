package com.ajjpj.concurrent.pool.api;


public interface AThreadPoolWithAdmin extends AThreadPool {
    AThreadPoolStatistics getStatistics();

    boolean isShutdown();
    void shutdown();
}
