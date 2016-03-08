package com.ajjpj.concurrent.pool._05_only_scanning_thread_clears_flag;


public enum SharedQueueStrategy {
    SyncPush,
    LockPush,
    NonBlockingPush
}
