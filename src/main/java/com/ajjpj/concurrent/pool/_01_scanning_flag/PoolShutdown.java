package com.ajjpj.concurrent.pool._01_scanning_flag;

/**
 * This Throwable is thrown by a special 'shutdown' task to signal worker threads to
 *  shut down without burdening regular execution.
 *
 * @author arno
 */
class PoolShutdown extends Error {
    @Override public Throwable fillInStackTrace () {
        return this;
    }
}