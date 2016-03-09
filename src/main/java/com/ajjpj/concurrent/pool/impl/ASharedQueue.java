package com.ajjpj.concurrent.pool.impl;


/**
 * @author arno
 */
public interface ASharedQueue {
    Runnable popFifo ();
    void push (Runnable task);

    /**
     * This is an optional method, used only for providing statistics data to aid monitoring and debugging. Implementations should therefore
     *  not require any compromise in regular push and pop performance.
     *
     * @return an approximation to the queue's current size
     */
    int approximateSize ();
}
