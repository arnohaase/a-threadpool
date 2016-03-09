package com.ajjpj.concurrent.pool.impl;


public enum SharedQueueStrategy {
    SyncPush,
    LockPush,
    NonBlockingPush
}
