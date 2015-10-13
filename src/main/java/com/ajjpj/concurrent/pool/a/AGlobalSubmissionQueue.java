package com.ajjpj.concurrent.pool.a;

/**
 * @author arno
 */
public interface AGlobalSubmissionQueue {
    void submit (Runnable task);
}
