package com.ajjpj.concurrent.pool.impl;

import com.ajjpj.afoundation.function.AFunction0NoThrow;
import com.ajjpj.afoundation.function.AFunction1NoThrow;
import com.ajjpj.afoundation.function.AStatement1NoThrow;
import com.ajjpj.afoundation.util.AUnchecker;
import com.ajjpj.concurrent.pool.api.*;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;


/**
 * @author arno
 */
public class AThreadPoolImpl implements AThreadPoolWithAdmin {
    /**
     * This is a compile-time switch to completely remove all statistics gathering for the really paranoid
     */
    public static final boolean SHOULD_GATHER_STATISTICS = true;


    /**
     * This long is a bit set with the indexes of threads that are currently idling. All idling threads are guaranteed to be in this set,
     *  but some threads may be marked as idle though they are still settling down or otherwise not quite idle. All modifications are
     *  done as CAS via UNSAFE.<p>
     * This mechanism allows an optimization when threads are unparked: only threads marked as idle need to be unparked, and only one
     *  volatile read is required rather than one per worker thread.
     * TODO is this really faster, and if so, is the difference significant?
     * TODO this currently limits the number of threads to 64 --> generalize
     */
    @SuppressWarnings({"unused"})
    private volatile long idleThreads = 0;

    static final long MASK_IDLE_THREAD_SCANNING = Long.MIN_VALUE; // top-most bit reserved to signify 'scanning'

    private final ASharedQueue[] sharedQueues;
    final LocalQueue[] localQueues;

    private final Map<Integer, Integer> producerToQueueAffinity = new ConcurrentHashMap<> ();

    /**
     * this is a long rather than an int to be on the safe side - 2 billion different producer threads during the lifetime of a thread pool, but still...
     */
    private final AtomicLong nextSharedQueue = new AtomicLong (0);

    final AtomicBoolean shutdown = new AtomicBoolean (false);
    final boolean checkShutdownOnSubmission;

    public AThreadPoolImpl (boolean isDaemon, AFunction0NoThrow<String> threadNameFactory, AStatement1NoThrow<Throwable> exceptionHandler,
                            int numThreads, int localQueueSize, int numSharedQueues, boolean checkShutdownOnSubmission, AFunction1NoThrow<AThreadPoolImpl,ASharedQueue> sharedQueueFactory) {
        this.checkShutdownOnSubmission = checkShutdownOnSubmission;
        sharedQueues = new ASharedQueue[numSharedQueues];
        for (int i=0; i<numSharedQueues; i++) {
            sharedQueues[i] = sharedQueueFactory.apply (this);
        }

        final Set<Integer> sharedQueuePrimes = primeFactors (numSharedQueues);

        localQueues = new LocalQueue[numThreads];
        for (int i=0; i<numThreads; i++) {
            localQueues[i] = new LocalQueue (this, localQueueSize);
            final WorkerThread thread = new WorkerThread (localQueues[i], sharedQueues, this, i, prime (i, sharedQueuePrimes), exceptionHandler);
            thread.setDaemon (isDaemon);
            thread.setName (threadNameFactory.apply ());
            localQueues[i].init (thread);
        }

        for (LocalQueue queue: localQueues) {
            //noinspection ConstantConditions
            queue.thread.start ();
        }
    }

    /**
     * This method returns an approximation of statistical data for all worker threads since the pool was started. Updates of the statistical data is done without synchronization,
     *  so some or all of the data may be stale, and some numbers may be pretty outdated while others are very current, even for the same thread. For long-running pools however
     *  the data may be useful in analyzing behavior in general and performance anomalies in particular. Your mileage may vary, you have been warned! ;-)
     */
    @Override public AThreadPoolStatistics getStatistics() {
        final AWorkerThreadStatistics[] workerStats = new AWorkerThreadStatistics[localQueues.length];
        for (int i=0; i<localQueues.length; i++) {
            //noinspection ConstantConditions
            workerStats[i] = localQueues[i].thread.getStatistics ();
        }

        final ASharedQueueStatistics[] sharedQueueStats = new ASharedQueueStatistics[sharedQueues.length];
        for (int i=0; i<sharedQueues.length; i++) {
            sharedQueueStats[i] = new ASharedQueueStatistics (sharedQueues[i].approximateSize());
        }

        return new AThreadPoolStatistics (workerStats, sharedQueueStats);
    }

    static Set<Integer> primeFactors (int n) {
        final Set<Integer> result = new HashSet<> ();

        for (int i=2; i<=n; i++) {
            if (isPrime (i) && n%i == 0) {
                result.add (i);
            }
        }

        return result;
    }

    static int prime (int number, Set<Integer> exceptions) {
        int numPrevPrimes = 0;

        for (int candidate=1; candidate<Integer.MAX_VALUE; candidate++) {
            if (isPrime (candidate) && !exceptions.contains (candidate)) {
                if (numPrevPrimes >= number) {
                    return candidate;
                }
                numPrevPrimes += 1;
            }
        }
        return 1; // this should never happen, but '1' is a safe fallback
    }

    static boolean isPrime (int n) {
        for (int i=2; i<=n/2; i++) {
            if (n%i == 0) return false;
        }
        return true;
    }

    @Override public void submit (Runnable code) {
        if (checkShutdownOnSubmission && shutdown.get ()) { //TODO verify if this check incurs significant cost
            throw new IllegalStateException ("pool is already shut down");
        }

        WorkerThread wt;
        if (Thread.currentThread () instanceof WorkerThread && (wt = (WorkerThread) Thread.currentThread ()).pool == this) {
            if (SHOULD_GATHER_STATISTICS) wt.stat_numLocalSubmits += 1;
            try {
                wt.localQueue.push (code);
            }
            catch (RejectedExecutionException e) {
                sharedQueues[getSharedQueueForCurrentThread ()].push (code);
            }
        }
        else {
            sharedQueues[getSharedQueueForCurrentThread ()].push (code);
        }
    }

    private int getSharedQueueForCurrentThread() {
        final int key = System.identityHashCode (Thread.currentThread ());

        Integer result = producerToQueueAffinity.get (key);
        if (result == null) {
            if (producerToQueueAffinity.size () > 10_000) { //TODO make this number configurable
                // in the unusual situation that producers are transient, discard affinity data if the map gets too large
                producerToQueueAffinity.clear ();
            }

            result = (int) nextSharedQueue.getAndIncrement () % sharedQueues.length;
            producerToQueueAffinity.put (key, result);
        }

        return result;
    }

    @Override public State getState () {
        if (! shutdown.get ()) return State.Running;

        for (LocalQueue q: this.localQueues) {
            if (q.thread.isAlive ()) return State.ShuttingDown;
        }

        return State.Down;
    }

    /**
     * This method shuts down the thread pool. The method finishes immediately, returning a separate AFuture for every worker thread. In order to combine them into a single
     *  AFuture for all worker threads, use {@code AFuture.lift()} on the result.
     */
    @Override public List<AFuture<Void>> shutdown (ShutdownMode shutdownMode) {
        if (! shutdown.compareAndSet (false, true)) {
            throw new IllegalStateException ("pool can be shut down only once");
        }

        switch (shutdownMode) {
            case SkipUnstarted:
                for (ASharedQueue globalQueue: sharedQueues) {
                    //noinspection StatementWithEmptyBody
                    while (globalQueue.popFifo () != null) {
                        // do nothing, just drain the queue
                    }
                }

                for (LocalQueue queue: localQueues) {
                    //noinspection StatementWithEmptyBody
                    while (queue.popFifo () != null) {
                        // do nothing, just drain the queue
                    }
                }
                // fall-through is intentional
            case InterruptRunning:
                for (LocalQueue queue: localQueues) {
                    queue.thread.interrupt ();
                }
        }

        final List<AFuture<Void>> result = new ArrayList<> ();

        for (LocalQueue localQueue : localQueues) {
            final ASettableFuture<Void> f = ASettableFuture.create ();
            sharedQueues[0].push (() -> {
                throw new PoolShutdown (f);
            });
            UNSAFE.unpark (localQueue.thread);
            result.add (f);
        }

        return result;
    }


    void onAvailableTask () {
        long idleBitMask = UNSAFE.getLongVolatile (this, OFFS_IDLE_THREADS);
        if ((idleBitMask & MASK_IDLE_THREAD_SCANNING) != 0L) {
            // some other thread is scanning, so there is no need to wake another thread
            return;
        }
        doWakeUpWorker (idleBitMask);
    }

    void wakeUpWorker () {
        long idleBitMask = UNSAFE.getLongVolatile (this, OFFS_IDLE_THREADS);
        doWakeUpWorker (idleBitMask);
    }

    private void doWakeUpWorker (long idleBitMask) {
        if ((idleBitMask & ~MASK_IDLE_THREAD_SCANNING) == 0L) {
            // all threads are busy already
            return;
        }

        for (LocalQueue localQueue : localQueues) {
            if ((idleBitMask & 1L) != 0) {
                //noinspection ConstantConditions
                if (markWorkerAsBusyAndScanning (localQueue.thread)) {
                    // wake up the worker only if no-one else woke up the thread in the meantime
                    UNSAFE.unpark (localQueue.thread);
                }
                // even if someone else woke up the thread in the meantime, at least one thread is scanning --> we can safely abort here
                break;
            }
            idleBitMask = idleBitMask >> 1;
        }
    }

    void markWorkerAsIdle (long mask) {
        long prev, after;
        do {
            prev = UNSAFE.getLongVolatile (this, OFFS_IDLE_THREADS);
            after = prev | mask;

            // A 'scanning' thread (i.e. a thread that was triggered by 'onAvailableTask') going to sleep means that scanning is finished, so we
            //  can clear the flag. A thread going to sleep after doing some work also triggers the flag to be cleared, but that is safe and
            //  incurs little additional overhead - clearing the flag in this place is basically for free.
//            after = after & ~MASK_IDLE_THREAD_SCANNING;
        }
        while (! UNSAFE.compareAndSwapLong (this, OFFS_IDLE_THREADS, prev, after));
    }

    boolean markWorkerAsBusy (long mask) {
        long prev, after;
        do {
            prev = UNSAFE.getLongVolatile (this, OFFS_IDLE_THREADS);
            if ((prev & mask) == 0) {
                // someone else woke up the thread in the meantime
                return false;
            }

            after = prev & ~mask;
        }
        while (! UNSAFE.compareAndSwapLong (this, OFFS_IDLE_THREADS, prev, after));

        return true;
    }

    boolean markWorkerAsBusyAndScanning (WorkerThread worker) {
        final long mask = worker.idleThreadMask;
        long prev, after;
        do {
            prev = UNSAFE.getLongVolatile (this, OFFS_IDLE_THREADS); //TODO is a regular read more efficient here?
            if ((prev & mask) == 0L) {
                // someone else woke up the thread concurrently --> it is scanning now, and there is no need to wake it up or change the 'idle' mask
                return false;
            }

            after = prev & ~mask;
            after = after | MASK_IDLE_THREAD_SCANNING;
        }
        while (! UNSAFE.compareAndSwapLong (this, OFFS_IDLE_THREADS, prev, after));
        return true;
    }

    void unmarkScanning() {
        long prev, after;
        do {
            prev = UNSAFE.getLongVolatile (this, OFFS_IDLE_THREADS);
            after = prev & ~MASK_IDLE_THREAD_SCANNING;
            if (prev == after) {
                return;
            }
        }
        while (! UNSAFE.compareAndSwapLong (this, OFFS_IDLE_THREADS, prev, after));
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
