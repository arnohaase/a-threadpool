package com.ajjpj.concurrent.pool.b;

import com.ajjpj.afoundation.util.AUnchecker;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;


/**
 * @author arno
 */
public class AThreadPoolImpl {

    /**
     * This long is a bit set with the indexes of threads that are currently idling. All idling threads are guaranteed to be in this set,
     *  but some threads may be marked as idle though they are still settling down or otherwise not quite idle. All modifications are
     *  done as CAS via UNSAFE.<p>
     * This mechanism allows an optimization when threads are unparked: only threads marked as idle need to be unparked, and only one
     *  volatile read is required rather than one per worker thread.
     * TODO is this really faster, and if so, is the difference significant?
     * TODO this currently limits the number of threads to 64 --> generalize
     */
    @SuppressWarnings ("FieldCanBeLocal")
    private volatile long idleThreads = 0;

    private final ArrayBlockingQueue<AThreadPoolTask> globalQueue;
    final LocalQueue[] localQueues;

    volatile boolean shutdown;

    public AThreadPoolImpl (int numThreads, int localQueueSize, int globalQueueSize) {
        globalQueue = new ArrayBlockingQueue<> (globalQueueSize);

        localQueues = new LocalQueue[numThreads];
        for (int i=0; i<numThreads; i++) {
            localQueues[i] = new LocalQueue (this, localQueueSize);
            final WorkerThread thread = new WorkerThread (localQueues[i], globalQueue, this, i);
            localQueues[i].init (thread);
        }

        for (LocalQueue queue: localQueues) {
            //noinspection ConstantConditions
            queue.thread.start ();
        }
    }

    public <T> Future<T> submit (Callable<T> code) {
        if (shutdown) {
            throw new IllegalStateException ("pool is already shut down");
        }

        final AThreadPoolTask<T> task = new AThreadPoolTask<> (code);
        globalQueue.add (task);
        if (globalQueue.size () < 2) {
            onStealableTask ();
        }

        return task.future;
    }

    public void shutdown() {
        if (shutdown) {
            return;
        }

        shutdown = true;
        globalQueue.clear ();

        for (LocalQueue queue: localQueues) {
            //noinspection StatementWithEmptyBody
            while (queue.popFifo () != null) {
                // do nothing, just drain the queue
            }
        }

        for (LocalQueue localQueue : localQueues) {
            globalQueue.add (new AThreadPoolTask<> (() -> {
                throw new PoolShutdown ();
            }));
            UNSAFE.unpark (localQueue.thread);
        }
    }

    public void awaitTermination() throws InterruptedException {
        for (LocalQueue queue: localQueues) {
            //noinspection ConstantConditions
            queue.thread.join ();
        }
    }


    void onStealableTask () {
        long idleBitMask = idleThreads;

        for (LocalQueue localQueue : localQueues) {
            if ((idleBitMask & 1L) != 0) {
                UNSAFE.unpark (localQueue.thread);
            }
            idleBitMask = idleBitMask >> 1;
        }
    }

    void markWorkerAsIdle (long mask) {
        long prev;
        do {
            prev = UNSAFE.getLongVolatile (this, OFFS_IDLE_THREADS);
        }
        while (! UNSAFE.compareAndSwapLong (this, OFFS_IDLE_THREADS, prev, prev | mask));
    }

    void markWorkerAsUnIdle (long mask) {
        long prev;
        do {
            prev = UNSAFE.getLongVolatile (this, OFFS_IDLE_THREADS);
        }
        while (! UNSAFE.compareAndSwapLong (this, OFFS_IDLE_THREADS, prev, prev & ~mask));
    }

    //------------------ Unsafe stuff
    private static final Unsafe UNSAFE;

    private static final long OFFS_IDLE_THREADS;

    static {
        try {
            final Field f = Unsafe.class.getDeclaredField ("theUnsafe");
            f.setAccessible (true);
            UNSAFE = (Unsafe) f.get (null);

            OFFS_IDLE_THREADS = UNSAFE.objectFieldOffset (AThreadPoolImpl.class.getDeclaredField ("idleThreads"));
        }
        catch (Exception e) {
            AUnchecker.throwUnchecked (e);
            throw new RuntimeException(); // for the compiler
        }
    }

}
