package com.ajjpj.concurrent.pool.api.exc;

import java.util.concurrent.TimeoutException;


public class TimeoutExceptionWithoutStackTrace extends TimeoutException {

    @Override public Throwable fillInStackTrace () {
        return this;
    }
}
