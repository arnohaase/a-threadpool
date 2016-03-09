package com.ajjpj.concurrent.pool.impl;

import com.ajjpj.afoundation.function.AFunction1NoThrow;
import com.ajjpj.afoundation.function.AStatement1;
import com.ajjpj.concurrent.pool.api.AThreadPoolWithAdmin;


//TODO no-steal implementation
//TODO separate implementation optimized for blocking
//TODO configurable starvation avoidance: steal, shared
//TODO naming strategy
//TODO error handling strategy

public class AThreadPoolBuilder {
    private int numThreads = Runtime.getRuntime ().availableProcessors ();
    private int numSharedQueues = Runtime.getRuntime ().availableProcessors ();
    private int localQueueSize = 16384; //TODO smaller default; handle overflow so that it pushes to shared queue instead
    private int sharedQueueSize = 16384;

    private SharedQueueStrategy sharedQueueStrategy = SharedQueueStrategy.SyncPush;

    private AFunction1NoThrow<AThreadPoolImpl,ASharedQueue> sharedQueueFactory = pool -> {
        switch (sharedQueueStrategy) {
            case SyncPush: return new SharedQueueBlockPushBlockPopImpl (pool, sharedQueueSize);
            case LockPush: return new SharedQueueNonblockPushBlockPopImpl (pool, sharedQueueSize);
            case NonBlockingPush: return new SharedQueueNonBlockingImpl (pool, sharedQueueSize);
        }
        throw new IllegalStateException ("unknown shared queue strategy " + sharedQueueStrategy);
    };

    public AThreadPoolBuilder withNumThreads (int numThreads) {
        this.numThreads = numThreads;
        return this;
    }

    public AThreadPoolBuilder withNumSharedQueues (int numSharedQueues) {
        this.numSharedQueues = numSharedQueues;
        return this;
    }

    public AThreadPoolBuilder withLocalQueueSize (int localQueueSize) {
        this.localQueueSize = localQueueSize;
        return this;
    }

    public AThreadPoolBuilder withSharedQueueSize (int sharedQueueSize) {
        this.sharedQueueSize = sharedQueueSize;
        return this;
    }

    public AThreadPoolBuilder withSharedQueueStrategy (SharedQueueStrategy strategy) {
        this.sharedQueueStrategy = strategy;
        return this;
    }

    /**
     * Completely replaces shared factory creation by custom code. NB: While providing maximum control, calling this method requires a deep understanding
     *  of the interaction between a shared queue and its thread pool. If you are not sure what that means, you should probably not be using this method.
     */
    public AThreadPoolBuilder withSharedQueueFactory (AFunction1NoThrow<AThreadPoolImpl, ASharedQueue> sharedQueueFactory) {
        this.sharedQueueFactory = sharedQueueFactory;
        return this;
    }

    public <T extends Throwable> AThreadPoolBuilder log (AStatement1<String, T> logOperation) throws T {
        final String stringRepresentation = toString ();
        logOperation.apply (stringRepresentation);
        return this;
    }

    public AThreadPoolWithAdmin build() {
        //TODO log configuration
        return new AThreadPoolImpl (numThreads, localQueueSize, numSharedQueues, sharedQueueFactory);
    }

    @Override
    public String toString () {
        return "AThreadPoolBuilder{" +
                "numThreads=" + numThreads +
                ", numSharedQueues=" + numSharedQueues +
                ", localQueueSize=" + localQueueSize +
                ", sharedQueueSize=" + sharedQueueSize +
                ", sharedQueueStrategy=" + sharedQueueStrategy +
                ", sharedQueueFactory=" + sharedQueueFactory +
                '}';
    }
}
