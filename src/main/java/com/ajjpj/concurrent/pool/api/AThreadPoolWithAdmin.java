package com.ajjpj.concurrent.pool.api;


import java.util.List;


public interface AThreadPoolWithAdmin extends AThreadPool {
    enum State { Rrunning, ShuttingDown, Down}
    enum ShutdownMode { ExecuteSubmitted, SkipUnstarted, InterruptRunning }

    AThreadPoolStatistics getStatistics();

    State getState();
    List<AFuture<Void>> shutdown (ShutdownMode shutdownMode);
}
