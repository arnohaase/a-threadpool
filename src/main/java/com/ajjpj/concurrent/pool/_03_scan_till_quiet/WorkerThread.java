package com.ajjpj.concurrent.pool._03_scan_till_quiet;

import com.ajjpj.afoundation.util.AUnchecker;
import com.ajjpj.concurrent.pool.AWorkerThreadStatistics;
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
        idleThreadMask = 1L << (AThreadPoolImpl.NUM_SCANNING_BITS + threadIdx);
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
                AThreadPoolTask task;

                //TODO intermittently read from global localQueue(s) and FIFO end of local localQueue
                if ((task = tryGetWork ()) != null) {
                    if (AThreadPoolImpl.SHOULD_GATHER_STATISTICS) stat_numTasksExecuted += 1;
                    task.execute ();
                }
                else {
                    // spin a little before parking
                    pool.registerScanningThread();
                    if ((task = tryScan()) != null) {
                        if (AThreadPoolImpl.SHOULD_GATHER_STATISTICS) stat_numTasksExecuted += 1;
                        pool.unregisterScanningThread();
                        pool.onAvailableTask(); // wake up some other thread (TODO is this necessary?)
                        task.execute ();
                        continue topLevelLoop;
                    }

                    pool.markWorkerAsIdle (idleThreadMask);

                    // re-check availability of work after marking the thread as idle --> avoid races
                    if ((task = tryGetForeignWork ()) != null) {
                        if (pool.markWorkerAsBusy (idleThreadMask)) {
                            // thread was 'woken up' because of available work --> cause some other thread to be notified instead
                            pool.onAvailableTask ();
                        }
                        if (AThreadPoolImpl.SHOULD_GATHER_STATISTICS) stat_numTasksExecuted += 1;
                        task.execute ();
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
                        pool.wakeUpWorker ();
                        if (AThreadPoolImpl.SHOULD_GATHER_STATISTICS) stat_numTasksExecuted += 1;
                        task.execute ();
                    }
                }
            }
            catch (Exception e) {
                if (AThreadPoolImpl.SHOULD_GATHER_STATISTICS) stat_numExceptions += 1;
                //TODO error handling
                e.printStackTrace ();
            }
            catch (PoolShutdown e) {
                return;
            }
        }
    }

    //TODO NB: This inlining apparently costs around 10% of toal throughput(!)
    private AThreadPoolTask tryScan() {
        long sumOfTops=0;
        long prevSumOfTops=-1;

        int pass=0;
        //TODO make num of passes configurable
        while (pass<5 || prevSumOfTops != sumOfTops) {
            pass += 1;
            prevSumOfTops = sumOfTops;
            sumOfTops = 0;

            final int prevQueue = currentSharedQueue;
            //noinspection ForLoopReplaceableByForEach
            for (int i=0; i<globalQueues.length; i++) {
                final SharedQueue curQueue = globalQueues[currentSharedQueue];
                while (true) {
                    final long _base = UNSAFE.getLongVolatile (curQueue, SharedQueue.OFFS_BASE);
                    final long _top = UNSAFE.getLongVolatile (curQueue, SharedQueue.OFFS_TOP);

                    if (_base == _top) {
                        // Terminate the loop: the queue is empty.
                        //TODO verify that Hotspot optimizes this kind of return-from-the-middle well
                        sumOfTops += _top;
                        break;
                    }

                    if (_top > _base && UNSAFE.compareAndSwapLong (curQueue, SharedQueue.OFFS_BASE, _base, _base+1)) {
                        if (AThreadPoolImpl.SHOULD_GATHER_STATISTICS) stat_numSharedTasksExecuted += 1;
                        //noinspection PointlessBooleanExpression,ConstantConditions
                        if (AThreadPoolImpl.SHOULD_GATHER_STATISTICS && prevQueue != currentSharedQueue) stat_numSharedQueueSwitches += 1;
                        return (AThreadPoolTask) UNSAFE.getAndSetObject (curQueue.tasks, curQueue.taskOffset (_base), null);
                    }
                }
                currentSharedQueue = (currentSharedQueue + queueTraversalIncrement) % globalQueues.length;
            }

            for (LocalQueue otherQueue: allLocalQueues) {
                //TODO optimization: different starting points per thread
                if (otherQueue == localQueue) {
                    continue;
                }

                while (true) {
                    // reading 'base' with volatile semantics emits the necessary barriers to ensure visibility of 'top'
                    final long _base = UNSAFE.getLongVolatile (localQueue, LocalQueue.OFFS_BASE);
                    final long _top = localQueue.top;

                    if (_base == _top) {
                        // Terminate the loop: the queue is empty.
                        //TODO verify that Hotspot optimizes this kind of return-from-the-middle well
                        sumOfTops += _top;
                        break;
                    }

                    // a regular read is OK here: 'push()' emits a store barrier after storing the task, 'popLifo()' modifies it with CAS, and 'popFifo()' does
                    //  a volatile read of 'base' before reading the task
                    final AThreadPoolTask result = localQueue.tasks[localQueue.asArrayindex (_base)];

                    // 'null' means that another thread concurrently fetched the task from under our nose. CAS ensures that only one thread
                    //  gets the task, and allows GC when processing is finished
                    if (result != null && _base == UNSAFE.getLongVolatile(localQueue, LocalQueue.OFFS_BASE) && UNSAFE.compareAndSwapObject (localQueue.tasks, localQueue.taskOffset (_base), result, null)) {
                        //TODO Why does re-reading and comparing 'base' avoid the wrap-around race? Apparently it does (idea gleaned from FJ)
                        UNSAFE.putLongVolatile (localQueue, LocalQueue.OFFS_BASE, _base+1); //TODO is 'putOrdered' sufficient?
                        if (AThreadPoolImpl.SHOULD_GATHER_STATISTICS) stat_numSteals += 1;
                        return result;
                    }
                }
            }
        }
        return null;
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

    private AThreadPoolTask tryGetForeignWork () {
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

    private AThreadPoolTask tryStealWork () {
        AThreadPoolTask task;
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
