package com.ajjpj.concurrent.pool.b;

import com.ajjpj.afoundation.util.AUnchecker;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;


/**
 * @author arno
 */
public class AThreadPoolImpl {
    public static final boolean SHOULD_GATHER_STATISTICS = true; // compile-time switch to enable / disable statistics gathering


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

    private final SharedQueue[] globalQueues;
    final LocalQueue[] localQueues;

    private final Map<Integer, Integer> producerToQueueAffinity = new ConcurrentHashMap<> ();

    /**
     * this is a long rather than an int to be on the safe side - 2 billion different producer threads during the lifetime of a thread pool, but still...
     */
    private final AtomicLong nextSharedQueue = new AtomicLong (0);

    volatile boolean shutdown;

    public AThreadPoolImpl (int numThreads, int localQueueSize, int globalQueueSize) {
        this (numThreads, localQueueSize, globalQueueSize, Runtime.getRuntime ().availableProcessors ());
    }

    public AThreadPoolImpl (int numThreads, int localQueueSize, int globalQueueSize, int numSharedQueues) {
        globalQueues = new SharedQueue[numSharedQueues];
        for (int i=0; i<numSharedQueues; i++) {
            globalQueues[i] = new SharedQueue (this, globalQueueSize);
        }

        final Set<Integer> sharedQueuePrimes = primeFactors (numSharedQueues);

        localQueues = new LocalQueue[numThreads];
        for (int i=0; i<numThreads; i++) {
            localQueues[i] = new LocalQueue (this, localQueueSize);
            final WorkerThread thread = new WorkerThread (localQueues[i], globalQueues, this, i, prime (i, sharedQueuePrimes));
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
    public WorkerThreadStatistics[] getStatistics() {
        final WorkerThreadStatistics[] result = new WorkerThreadStatistics[localQueues.length];
        for (int i=0; i<localQueues.length; i++) {
            //noinspection ConstantConditions
            result[i] = localQueues[i].thread.getStatistics ();
        }
        return result;
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
        throw new IllegalStateException (); //TODO
    }

    static boolean isPrime (int n) {
        for (int i=2; i<=n/2; i++) {
            if (n%i == 0) return false;
        }
        return true;
    }

    public <T> Future<T> submit (Callable<T> code) {
        if (shutdown) {
            throw new IllegalStateException ("pool is already shut down");
        }

        final AThreadPoolTask<T> task = new AThreadPoolTask<> (code);

        WorkerThread wt;
        if (Thread.currentThread () instanceof WorkerThread && (wt = (WorkerThread) Thread.currentThread ()).pool == this) {
            if (SHOULD_GATHER_STATISTICS) wt.stat_numLocalSubmits += 1;
            wt.localQueue.push (task);
        }
        else {
            globalQueues[getSharedQueueForCurrentThread ()].push (task);
        }

        return task.future;
    }

    private int getSharedQueueForCurrentThread() {
        final int key = System.identityHashCode (Thread.currentThread ());

        Integer result = producerToQueueAffinity.get (key);
        if (result == null) {
            if (producerToQueueAffinity.size () > 10_000) { //TODO make this number configurable
                // in the unusual situation that producers are transient, discard affinity data if the map gets too large
                producerToQueueAffinity.clear ();
            }

            result = (int) nextSharedQueue.getAndIncrement () % globalQueues.length;
            producerToQueueAffinity.put (key, result);
        }

        return result;
    }

    public void shutdown() {
        if (shutdown) {
            return;
        }

        //TODO flag for 'finish submitted work'
        //TODO clean completion of submitted and cancelled tasks

        shutdown = true;

        for (SharedQueue globalQueue: globalQueues) {
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

        for (LocalQueue localQueue : localQueues) {
            globalQueues[0].push (new AThreadPoolTask<> (() -> {
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


    void onAvailableTask () {
        long idleBitMask = UNSAFE.getLongVolatile (this, OFFS_IDLE_THREADS);

        if (idleBitMask == 0) {
            return;
        }

//        if (idleBitMask != 0) {
//            System.err.println ("unparking: " + idleBitMask);
//        }

        for (LocalQueue localQueue : localQueues) {
            if ((idleBitMask & 1L) != 0) {
                //noinspection ConstantConditions
                markWorkerAsUnIdle (localQueue.thread.idleThreadMask);
//                System.err.println ("unparking " + localQueue.thread.getName ());
                UNSAFE.unpark (localQueue.thread);
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
//            System.err.println ("setting idle " + mask + " on " + prev);
        }
        while (! UNSAFE.compareAndSwapLong (this, OFFS_IDLE_THREADS, prev, after));

//        System.err.println ("mark as idle: " + prev + " -> " + after + " @ " + mask);
    }

    void markWorkerAsUnIdle (long mask) {
        long prev, after;
        do {
            prev = UNSAFE.getLongVolatile (this, OFFS_IDLE_THREADS);
            after = prev & ~mask;
//            System.err.println ("setting unidle " + mask + " on " + prev);
        }
        while (! UNSAFE.compareAndSwapLong (this, OFFS_IDLE_THREADS, prev, after));

//        System.err.println ("mark as unidle: " + prev + " -> " + after + " @ " + mask);
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
