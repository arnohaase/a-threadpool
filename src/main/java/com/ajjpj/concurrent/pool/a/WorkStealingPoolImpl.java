package com.ajjpj.concurrent.pool.a;

import com.ajjpj.concurrent.pool.AFuture;
import com.ajjpj.concurrent.pool.APool;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicLong;


/**
 *
 *
 * optional 'no work stealing' policy
 *
 * intermittent fetching of work from global queue even if there is work in local queues --> avoid starvation
 *
 * availableWorkers: 'stack' semantics is intentional - reuse most recently parked thread first
 *
 * @author arno
 */
public class WorkStealingPoolImpl implements APool {
    final WorkStealingThread[] threads;
    final WorkStealingLocalQueue[] localQueues;
    final WorkStealingGlobalQueue globalQueue;

    private final CountDownLatch shutdownLatch;

    static final ASubmittable CHECK_QUEUES = new ASubmittable (null, null);

    public WorkStealingPoolImpl (int numThreads) { //TODO move default values to Builder class
        this (numThreads, 16384, 16384, 100, 1, 100);
    }

    public WorkStealingPoolImpl (int numThreads, int globalCapacity, int localCapacity, int globalBeforeLocalInterval, int numPollsBeforePark, int pollNanosBeforePark) {
        this.globalQueue = new WorkStealingGlobalQueue (globalCapacity);
        this.shutdownLatch = new CountDownLatch (numThreads);

        this.localQueues = new WorkStealingLocalQueue[numThreads];
        this.threads     = new WorkStealingThread [numThreads];
        for (int i=0; i<numThreads; i++) {
            threads[i] = new WorkStealingThread (this, i, localCapacity, globalBeforeLocalInterval, numPollsBeforePark, pollNanosBeforePark);
            localQueues[i] = threads[i].queue;
        }
    }

    /**
     * This method actually starts the threads. It is separate from the constructor to ensure safe publication of final state.
     */
    public WorkStealingPoolImpl start () {
        for (Thread thread: threads) {
            thread.start ();
        }

        return this;
    }

    @Override public <T> AFuture<T> submit (Callable<T> code) {
        final ATask<T> result = new ATask<> ();
        final ASubmittable submittable = new ASubmittable (result, code);

        doSubmit (submittable);

        return result;
    }

    void doSubmit (ASubmittable task) {
        //TODO ensure that 'shutdown' is properly detected
        try {
            if (! tryWakeUpWorkerWith (task)) {
//            if (wakeUpWorkerWith (submittable)) {
//                if (shouldCollectStatistics) numWakeups.incrementAndGet ();
//            }
//            else {
                final Thread curThread = Thread.currentThread ();
                if (curThread instanceof WorkStealingThread && ((WorkStealingThread) curThread).pool == this) {
                    if (shouldCollectStatistics) numLocalPush.incrementAndGet ();
                    ((WorkStealingThread) curThread).queue.submit (task);
                }
                else {
                    if (shouldCollectStatistics) numGlobalPush.incrementAndGet ();
//                    System.out.println ("before submit global");

                    // 'submit' first attempts to shortcut by waking up an available worker thread. That code was moved to the global queue
                    //  so that the lock must be acquired only once, ensuring atomicity at the same time.
                    globalQueue.submit (task);
//                    System.out.println ("after submit global");

//                    System.out.println ("global push: waking up worker");
                    // Wake up a worker to check queues. This handles the rare race condition that all workers went to sleep since the check at the beginning of this method
//                    wakeUpWorkerWith (CHECK_QUEUES);
//                    System.out.println ("  after global push");
                    //TODO do this for local work too?
                    if (tryWakeUpWorkerWith (CHECK_QUEUES)) {
                        System.out.println ("+-");
                    }
                }
            }
        }
        catch (WorkStealingShutdownException e) {
            throw new RejectedExecutionException ("pool is shut down");
        }
    }

    private boolean tryWakeUpWorkerWith (ASubmittable task) {
        for (WorkStealingThread worker: threads) {
            if (worker.wakeUpWith (task)) {
                return true;
            }
        }
        return false;
    }

    //TODO clean this up
    void onThreadFinished (WorkStealingThread thread) {
        shutdownLatch.countDown ();
    }

    @Override public void shutdown () throws InterruptedException {
        globalQueue.shutdown ();
        for (WorkStealingLocalQueue q: localQueues) {
            q.shutdown ();
        }

//        WorkStealingThread worker;
//        while ((worker = availableWorker ()) != null) {
//            worker.wakeUpWith (SHUTDOWN);
//        }
//
        for (WorkStealingThread thread: threads) {
            thread.wakeUpWith (null); //TODO this may need more aggressive code
        }

        shutdownLatch.await ();
    }

    //----------------------------------- statistics

    static final boolean shouldCollectStatistics = true;

    final AtomicLong numWakeups = new AtomicLong ();
    final AtomicLong numGlobalPush = new AtomicLong ();
    final AtomicLong numLocalPush = new AtomicLong ();

    public long getNumWakeups() {
        return numWakeups.get ();
    }

    public long getNumGlobalPushs() {
        return numGlobalPush.get ();
    }

    public long getNumLocalPushs() {
        return numLocalPush.get ();
    }

    //----------------------------------- internal data structure for submitted task

    static class ASubmittable implements Runnable {
        static final long UNQUEUED = -1;

        private final ATask result;
        private final Callable code;
        long queueIndex;

        ASubmittable (ATask result, Callable code) {
            this (result, code, UNQUEUED);
        }

        ASubmittable (ATask result, Callable code, long queueIndex) {
            this.result = result;
            this.code = code;
            this.queueIndex = queueIndex;
        }

        @SuppressWarnings ("unchecked")
        @Override public void run () {
            try {
                result.set (code.call ());
            }
            catch (Throwable th) {
                result.setException (th);
            }
        }

        ASubmittable withQueueIndex (long queueIndex) {
            this.queueIndex = queueIndex;
            return this;
//            return new ASubmittable (result, code, queueIndex);
        }
    }
}
