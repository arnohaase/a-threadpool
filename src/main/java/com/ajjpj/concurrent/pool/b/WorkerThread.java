package com.ajjpj.concurrent.pool.b;

import com.ajjpj.afoundation.util.AUnchecker;
import sun.misc.Unsafe;

import java.lang.reflect.Field;


/**
 * @author arno
 */
class WorkerThread extends Thread {
    final LocalQueue localQueue;
    final SharedQueue[] globalQueues;
    final LocalQueue[] allLocalQueues;
    final AThreadPoolImpl pool;
    final long idleThreadMask;
    final int queueTraversalIncrement;

    /**
     * This is the index of the shared queue that this thread currently feeds from.
     */
    private int currentSharedQueue = 0;

    WorkerThread (LocalQueue localQueue, SharedQueue[] globalQueues, AThreadPoolImpl pool, int threadIdx, int queueTraversalIncrement) {
        this.localQueue = localQueue;
        this.globalQueues = globalQueues;
        this.pool = pool;
        this.allLocalQueues = pool.localQueues;
        idleThreadMask = 1L << threadIdx;
        this.queueTraversalIncrement = queueTraversalIncrement;
    }

    @Override public void run () {
        topLevelLoop:
        while (true) {
            try {
                AThreadPoolTask task;

                //TODO intermittently read from global localQueue(s) and FIFO end of local localQueue
                if ((task = tryGetWork ()) != null) {
                    task.execute ();
                }
                else {
                    // spin a little before parking
                    for (int i=0; i<200; i++) { //TODO make this configurable, optimize, benchmark, ...
                        if ((task = tryGetForeignWork ()) != null) {
                            task.execute ();
                            continue topLevelLoop;
                        }
                    }

                    pool.markWorkerAsIdle (idleThreadMask);

                    // re-check availability of work after marking the thread as idle --> avoid races
                    if ((task = tryGetForeignWork ()) != null) {
                        pool.markWorkerAsUnIdle (idleThreadMask);
                        task.execute ();
                        continue;
                    }

                    UNSAFE.park (false, 0L);

                    if ((task = tryGetForeignWork ()) != null) {
                        pool.onAvailableTask ();
                        task.execute ();
                    }
                }
            }
            catch (Exception e) {
                //TODO error handling
                e.printStackTrace ();
            }
            catch (PoolShutdown e) {
                return;
            }
        }
    }

    private AThreadPoolTask tryGetWork() {
        AThreadPoolTask task;

        //TODO intermittently read from global localQueue(s) and FIFO end of local localQueue
        if ((task = localQueue.popLifo ()) != null) {
            return task;
        }
        else if ((task = tryGetSharedWork ()) != null) {
            return task;
        }
        else if ((task = tryStealWork ()) != null) {
            return task;
        }
        return null;
    }

    private AThreadPoolTask tryGetForeignWork() {
        AThreadPoolTask task;

        if ((task = tryGetSharedWork ()) != null) {
            return task;
        }
        else if ((task = tryStealWork ()) != null) {
            return task;
        }
        return null;
    }

    private AThreadPoolTask tryGetSharedWork() {
        AThreadPoolTask task;

        //TODO optimization: different starting points per thread
        //TODO go forward once in a while to avoid starvation

        //noinspection ForLoopReplaceableByForEach
        for (int i=0; i<globalQueues.length; i++) {
            if ((task = globalQueues[currentSharedQueue].popFifo ()) != null) {
                return task;
            }
            currentSharedQueue = (currentSharedQueue + queueTraversalIncrement) % globalQueues.length;
        }

        return null;
    }

    private AThreadPoolTask tryStealWork () {
        AThreadPoolTask task;
        for (LocalQueue otherQueue: allLocalQueues) {
            //TODO optimization: different starting points per thread
            if (otherQueue == localQueue) {
                continue;
            }
            if ((task = otherQueue.popFifo ()) != null) {
                return task;
            }
        }

        return null;
    }

    //-------------------- Unsafe stuff
    private static final Unsafe UNSAFE;

    static {
        try {
            final Field f = Unsafe.class.getDeclaredField ("theUnsafe");
            f.setAccessible (true);
            UNSAFE = (Unsafe) f.get (null);
        }
        catch (Exception exc) {
            AUnchecker.throwUnchecked (exc);
            throw new RuntimeException(); // for the compiler
        }
    }

}
