package com.ajjpj.concurrent.pool.api.exc;

import java.util.concurrent.RejectedExecutionException;


public class RejectedExecutionExceptionWithoutStacktrace extends RejectedExecutionException {
    public RejectedExecutionExceptionWithoutStacktrace () {
    }

    public RejectedExecutionExceptionWithoutStacktrace (String msg) {
        super (msg);
    }
}
