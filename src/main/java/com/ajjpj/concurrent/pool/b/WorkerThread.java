package com.ajjpj.concurrent.pool.b;

import com.ajjpj.afoundation.util.AUnchecker;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;


/**
 * @author arno
 */
class WorkerThread extends Thread {
    final LocalQueue localQueue;
    final BlockingQueue<AThreadPoolTask> globalQueue;
    final LocalQueue[] allLocalQueues;
    final AThreadPoolImpl pool;
    final long idleThreadMask;

    WorkerThread (LocalQueue localQueue, BlockingQueue<AThreadPoolTask> globalQueue, AThreadPoolImpl pool, int threadIdx) {
        this.localQueue = localQueue;
        this.globalQueue = globalQueue;
        this.pool = pool;
        this.allLocalQueues = pool.localQueues;
        idleThreadMask = 1L << threadIdx;
    }

    @Override public void run () {
        while (true) {
            try {
                AThreadPoolTask task;

                //TODO intermittently read from global localQueue(s) and FIFO end of local localQueue
                if ((task = localQueue.popLifo ()) != null) {
                    task.execute ();
                }
                else if ((task = tryFetchWork ()) != null) {
                    task.execute ();
                }
                else {
                    pool.markWorkerAsIdle (idleThreadMask);

                    // re-check availability of work to avoid races
                    if ((task = tryFetchWork ()) != null) {
                        pool.markWorkerAsUnIdle (idleThreadMask);
                        task.execute ();
                        continue;
                    }

                    UNSAFE.park (false, 0L);
                    pool.markWorkerAsUnIdle (idleThreadMask);
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

    AThreadPoolTask tryFetchWork() {
        AThreadPoolTask task;
        if ((task = globalQueue.poll ()) != null) {
            return task;
        }

        for (LocalQueue otherQueue: allLocalQueues) {
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
