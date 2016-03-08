package com.ajjpj.concurrent.pool._05_only_scanning_thread_clears_flag;


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
