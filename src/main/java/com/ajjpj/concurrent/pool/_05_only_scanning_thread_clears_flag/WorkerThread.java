package com.ajjpj.concurrent.pool._05_only_scanning_thread_clears_flag;

import com.ajjpj.afoundation.util.AUnchecker;
import com.ajjpj.concurrent.pool.api.AWorkerThreadStatistics;
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

    //---------------------------------------------------
    //-- statistics data, written only from this thread
    //---------------------------------------------------

    long stat_numTasksExecuted = 0;
    long stat_numSharedTasksExecuted = 0;
    long stat_numSteals = 0;
    long stat_numExceptions = 0;

    long stat_numParks = 0;
    long stat_numFalseAlarmUnparks = 0;
    long stat_numSharedQueueSwitches = 0;

    long stat_numLocalSubmits = 0;

    /**
     * This is the index of the shared queue that this thread currently feeds from.
     */
    private int currentSharedQueue = 0; //TODO spread initial value across the range?

    WorkerThread (LocalQueue localQueue, SharedQueue[] globalQueues, AThreadPoolImpl pool, int threadIdx, int queueTraversalIncrement) {
        this.localQueue = localQueue;
        this.globalQueues = globalQueues;
        this.pool = pool;
        this.allLocalQueues = pool.localQueues;
        idleThreadMask = 1L << threadIdx;
        this.queueTraversalIncrement = queueTraversalIncrement;
    }

    /**
     * This method returns an approximation of this thread's execution statistics for the entire period since the thread was started. Writes are done without memory barriers
     *  to minimize the performance impact of statistics gathering, so some or all returned data may be arbitrarily stale, and some fields may be far staler than others. For
     *  long-running pools however even approximate data may provide useful insights. Your mileage may vary however, you have been warned ;-)
     */
    AWorkerThreadStatistics getStatistics() {
        return new AWorkerThreadStatistics (stat_numTasksExecuted, stat_numSharedTasksExecuted, stat_numSteals, stat_numExceptions, stat_numParks, stat_numFalseAlarmUnparks, stat_numSharedQueueSwitches, stat_numLocalSubmits, localQueue.approximateSize ());
    }

    @Override public void run () {
        long tasksAtPark = -1;

        topLevelLoop:
        while (true) {
            try {
                Runnable task;

                //TODO intermittently read from global localQueue(s) and FIFO end of local localQueue
                if ((task = tryGetWork ()) != null) {
                    if (AThreadPoolImpl.SHOULD_GATHER_STATISTICS) stat_numTasksExecuted += 1;
                    task.run ();
                }
                else {
                    // spin a little before parking
                    for (int i=0; i<00; i++) { //TODO make this configurable, optimize, benchmark, ...
                        if ((task = tryGetForeignWork ()) != null) {
                            if (AThreadPoolImpl.SHOULD_GATHER_STATISTICS) stat_numTasksExecuted += 1;
                            task.run ();
                            continue topLevelLoop;
                        }
                    }

                    pool.markWorkerAsIdle (idleThreadMask);

                    // re-check availability of work after marking the thread as idle --> avoid races
                    if ((task = tryGetForeignWork ()) != null) {
                        if (pool.markWorkerAsBusy (idleThreadMask)) {
                            // thread was 'woken up' because of available work --> cause some other thread to be notified instead
                            pool.onAvailableTask ();
                        }
                        if (AThreadPoolImpl.SHOULD_GATHER_STATISTICS) stat_numTasksExecuted += 1;
                        task.run ();
                        continue;
                    }

                    if (AThreadPoolImpl.SHOULD_GATHER_STATISTICS) stat_numParks += 1;
                    if (AThreadPoolImpl.SHOULD_GATHER_STATISTICS) {
                        if (tasksAtPark == stat_numTasksExecuted) {
                            stat_numFalseAlarmUnparks += 1;
                        }
                        else {
                            tasksAtPark = stat_numTasksExecuted;
                        }
                    }
                    UNSAFE.park (false, 0L);

                    if ((task = tryGetForeignWork ()) != null) {
                        pool.unmarkScanning(); //TODO why exactly is this necessary?
                        pool.wakeUpWorker ();
                        if (AThreadPoolImpl.SHOULD_GATHER_STATISTICS) stat_numTasksExecuted += 1;
                        task.run ();
                    }
                    else {
                        pool.unmarkScanning();
                    }
                }
            }
            catch (PoolShutdown e) {
                return;
            }
            catch (Throwable e) {
                if (AThreadPoolImpl.SHOULD_GATHER_STATISTICS) stat_numExceptions += 1;
                //TODO error handling
                e.printStackTrace ();
            }
        }
    }

    private Runnable tryGetWork() {
        Runnable task;

        //TODO intermittently read from global SharedQueue(s) and FIFO end of local localQueue
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

    private Runnable tryGetForeignWork () {
        Runnable task;

        if ((task = tryGetSharedWork ()) != null) {
            return task;
        }
        else if ((task = tryStealWork ()) != null) {
            return task;
        }
        return null;
    }

    private Runnable tryGetSharedWork() {
        Runnable task;

        //TODO optimization: different starting points per thread
        //TODO go forward once in a while to avoid starvation

        final int prevQueue = currentSharedQueue;

        //noinspection ForLoopReplaceableByForEach
        for (int i=0; i<globalQueues.length; i++) {
            if ((task = globalQueues[currentSharedQueue].popFifo ()) != null) {
                if (AThreadPoolImpl.SHOULD_GATHER_STATISTICS) stat_numSharedTasksExecuted += 1;
                //noinspection PointlessBooleanExpression,ConstantConditions
                if (AThreadPoolImpl.SHOULD_GATHER_STATISTICS && prevQueue != currentSharedQueue) stat_numSharedQueueSwitches += 1;
                return task;
            }
            currentSharedQueue = (currentSharedQueue + queueTraversalIncrement) % globalQueues.length;
        }

        return null;
    }

    private Runnable tryStealWork () {
        Runnable task;
        for (LocalQueue otherQueue: allLocalQueues) {
            //TODO optimization: different starting points per thread
            if (otherQueue == localQueue) {
                continue;
            }
            if ((task = otherQueue.popFifo ()) != null) {
                if (AThreadPoolImpl.SHOULD_GATHER_STATISTICS) stat_numSteals += 1;
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
