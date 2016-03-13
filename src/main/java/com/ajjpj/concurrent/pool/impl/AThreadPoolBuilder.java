package com.ajjpj.concurrent.pool.impl;

import com.ajjpj.afoundation.function.AFunction0NoThrow;
import com.ajjpj.afoundation.function.AFunction1NoThrow;
import com.ajjpj.afoundation.function.AStatement1;
import com.ajjpj.afoundation.function.AStatement1NoThrow;
import com.ajjpj.concurrent.pool.api.AThreadPoolWithAdmin;


//TODO no-steal implementation: WakeupForLocalTasks, StealOnIdle, NoStealing
//TODO separate implementation optimized for blocking
//TODO configurable starvation avoidance: steal, shared

public class AThreadPoolBuilder {
    private boolean checkShutdownOnSubmission = true;

    private int numThreads = Runtime.getRuntime ().availableProcessors ();
    private int numSharedQueues = Runtime.getRuntime ().availableProcessors ();
    private int localQueueSize = 16384; //TODO smaller default; handle overflow so that it pushes to shared queue instead
    private int sharedQueueSize = 16384;

    private SharedQueueStrategy sharedQueueStrategy = SharedQueueStrategy.SyncPush;

    private boolean isDaemon = false;
    private AFunction0NoThrow<String> threadNameFactory = new DefaultThreadNameFactory ("AThreadPool");
    private AStatement1NoThrow<Throwable> exceptionHandler = Throwable::printStackTrace;

    private AFunction1NoThrow<AThreadPoolImpl,ASharedQueue> sharedQueueFactory = pool -> {
        switch (sharedQueueStrategy) {
            case SyncPush: return new SharedQueueBlockPushBlockPopImpl (pool, sharedQueueSize);
            case LockPush: return new SharedQueueNonblockPushBlockPopImpl (pool, sharedQueueSize);
            case NonBlockingPush: return new SharedQueueNonBlockingImpl (pool, sharedQueueSize);
        }
        throw new IllegalStateException ("unknown shared queue strategy " + sharedQueueStrategy);
    };

    public AThreadPoolBuilder withCheckShutdownOnSubmission (boolean checkShutdownOnSubmission) {
        this.checkShutdownOnSubmission = checkShutdownOnSubmission;
        return this;
    }

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

    public AThreadPoolBuilder withDaemonThreads (boolean daemonThreads) {
        this.isDaemon = daemonThreads;
        return this;
    }

    public AThreadPoolBuilder withThreadNamePrefix (String threadNamePrefix) {
        this.threadNameFactory = new DefaultThreadNameFactory (threadNamePrefix);
        return this;
    }

    public AThreadPoolBuilder withThreadNameFactory (AFunction0NoThrow<String> threadNameFactory) {
        this.threadNameFactory = threadNameFactory;
        return this;
    }

    public AThreadPoolBuilder withExceptionHandler (AStatement1NoThrow<Throwable> exceptionHandler) {
        this.exceptionHandler = exceptionHandler;
        return this;
    }

    public <T extends Throwable> AThreadPoolBuilder log (AStatement1<String, T> logOperation) throws T {
        final String stringRepresentation = toString ();
        logOperation.apply (stringRepresentation);
        return this;
    }

    public AThreadPoolWithAdmin build() {
        //TODO log configuration
        return new AThreadPoolImpl (isDaemon, threadNameFactory, exceptionHandler, numThreads, localQueueSize, numSharedQueues, checkShutdownOnSubmission, sharedQueueFactory);
    }

    @Override
    public String toString () {
        return "AThreadPoolBuilder{" +
                "checkShutdownOnSubmission=" + checkShutdownOnSubmission +
                ", numThreads=" + numThreads +
                ", numSharedQueues=" + numSharedQueues +
                ", localQueueSize=" + localQueueSize +
                ", sharedQueueSize=" + sharedQueueSize +
                ", sharedQueueStrategy=" + sharedQueueStrategy +
                ", isDaemon=" + isDaemon +
                ", sharedQueueFactory=" + sharedQueueFactory +
                '}';
    }
}
