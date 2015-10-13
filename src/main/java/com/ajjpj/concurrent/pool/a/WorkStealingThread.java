package com.ajjpj.concurrent.pool.a;

import com.ajjpj.concurrent.pool.a.WorkStealingPoolImpl.ASubmittable;
import sun.misc.Contended;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.concurrent.locks.LockSupport;


/**
 * @author arno
 */
@Contended
class WorkStealingThread extends Thread {
    //TODO When the implementation is stable, ensure that all data fits into a single cache line.
    //TODO combine with LocalQueue into a single class for more data locality and less indirection?
    final WorkStealingLocalQueue queue;
    final WorkStealingPoolImpl pool;

    private final int ownThreadIndex;

    private final int globalBeforeLocalInterval;
    private final int numPollsBeforePark;
    private final int pollNanosBeforePark;

    //TODO configuration parameter for 'no work stealing'

    // is written with Unsafe.putOrderedObject
    @SuppressWarnings ("unused")
    private volatile ASubmittable wakeUpTask = null;

    /**
     * 0: polling
     * 1: parking and waiting, i.e. ready for being woken up with 'unpark'
     *
     * all read access is through Unsafe
     */
//    volatile int status = 0;

    //TODO optimization: is a lazySet sufficient for in-thread access as long as other threads use a volatile read? Is there a 'lazy CAS'?

    public WorkStealingThread (WorkStealingPoolImpl pool, int ownThreadIndex, int queueCapacity, int globalBeforeLocalInterval, int numPollsBeforePark, int pollNanosBeforePark) {
        this.pool = pool;
        this.ownThreadIndex = ownThreadIndex;
        this.queue = new WorkStealingLocalQueue (queueCapacity, true);

        this.globalBeforeLocalInterval = globalBeforeLocalInterval;
        this.numPollsBeforePark = numPollsBeforePark;
        this.pollNanosBeforePark = pollNanosBeforePark;
    }


    //TODO scheduling with code affinity (rather than data affinity)


    @Override public void run () {
        int msgCount = 0;

        while (true) {
            msgCount += 1;

            try {
                final boolean pollGlobalQueueFirst = msgCount % globalBeforeLocalInterval == 0;
                if (pollGlobalQueueFirst) {
                    // This is the exceptional case: Polling the global queue first once in a while avoids starvation of
                    //  work from the global queue. This is important in systems where locally produced work can saturate
                    //  the pool, e.g. in actor-based systems.
                    if (exec (tryGlobalFetch()) || exec (tryLocalFetch ())) continue;
                }
                else {
                    // This is the normal case: check for local work first, and only if there is no local work look in
                    //  the global queue
                    if (exec (tryLocalFetch()) || exec (tryGlobalFetch ())) continue;
                }

                if (exec (tryActiveWorkStealing ())) continue;

                waitForWork ();
            }
            catch (WorkStealingShutdownException exc) {
                // this exception signals that the thread pool was shut down

                //noinspection finally
                try {
                    pool.onThreadFinished (this);
                }
                catch (Throwable exc2) {
                    exc2.printStackTrace (); //TODO exception handling
                }
                finally {
                    //noinspection ReturnInsideFinallyBlock
                    return;
                }
            }
            catch (Exception exc) {
                exc.printStackTrace (); //TODO exception handling, InterruptedException in particular
            }
        }
    }

    private ASubmittable tryGlobalFetch () {
        return pool.globalQueue.poll ();
    }

    private ASubmittable tryLocalFetch () {
        return queue.nextLocalTask ();
    }

    private boolean exec (ASubmittable optTask) {
        if (optTask != null) {
            optTask.run ();
            return true;
        }
        return false;
    }

    private ASubmittable tryActiveWorkStealing () { //TODO remember the last queue successfully stolen from?
        for (int i=1; i<pool.localQueues.length; i++) { //TODO store this length in an attribute of this class?
            final int victimThreadIndex = (ownThreadIndex + i) % pool.localQueues.length;

            final ASubmittable stolenTask = pool.localQueues[victimThreadIndex].poll ();
            if (stolenTask != null) {
                return stolenTask;
            }
        }
        return null;
    }

    private void waitForWork () throws InterruptedException {
        // There is currently no work available for this thread. That means that there is currently not enough work for all
        //  worker threads, i.e. the pool is in a 'low load' situation.
        //
        // Now we want to park this thread until work becomes available. There are basically two ways of doing that, and they
        //  have different trade-offs: Pushing work from the producing thread (i.e. calling 'unpark' from the producing thread
        //  when work becomes available), or polling from this thread (i.e. waiting some time and checking for work, without
        //  involving any producing threads in this thread's scheduling).
        //
        // Unparking a thread incurs a significant overhead *for the caller of 'unpark'*. In the 'push' approach, a producer
        //  thread is burdened with this overhead, which can severely limit throughput for high-frequency producers. Polling
        //  on the other hand causes each idling thread to place an ongoing load on the system.
        //
        // The following code compromises, starting out by polling and then parking itself, waiting to be awakened by a
        //  'push' operation when work becomes available.

        for (int i=0; i<numPollsBeforePark; i++) {
            // wait a little while and look again before really going to sleep
            LockSupport.parkNanos (null, pollNanosBeforePark); //TODO replace with U.parkNanos --> avoid even touching the 'blocker' field?
            if (exec (tryGlobalFetch ()) || exec (tryActiveWorkStealing ())) return;
        }

        // This call checks for last-minute work under a global lock
        ASubmittable newTask = null;
//        ASubmittable newTask = pool.globalQueue.addWorkerToAvailables (this);

        // re-check for available work in queues to avoid a race condition.

        while (newTask == null) {
            queue.checkShutdown ();

//            U.putOrderedInt (this, OFFS_STATUS, 1);
//            status = 1;

            //TODO exception handling
//            System.out.println (Thread.currentThread ().getName () + ": park");
            LockSupport.park (Boolean.TRUE);
            newTask = wakeUpTask;
//            System.out.println ("--> " + (newTask == WorkStealingPoolImpl.CHECK_QUEUES) + " @ " + getName ());
//            System.out.println (Thread.currentThread ().getName () + ": unpark with " + newTask);

            // 'stealing' locally submitted work this way is effectively delayed by the 'parkNanos' call above

            if (newTask == null) {
                newTask = tryGlobalFetch ();
            }
            else if (newTask == WorkStealingPoolImpl.CHECK_QUEUES) {
//                queue.checkShutdown ();
//                System.out.println ("-+ @ " + getName ());
                newTask = tryGlobalFetch ();
            }
        }
        U.putOrderedObject (this, WAKE_UP_TASK, null);

        newTask.run ();
    }

    boolean wakeUpWith (ASubmittable task) {
        if (LockSupport.getBlocker (this) != null && U.compareAndSwapObject (this, WAKE_UP_TASK, null, task)) {
//            if (task == WorkStealingPoolImpl.CHECK_QUEUES) System.out.println ("*** " + (wakeUpTask == WorkStealingPoolImpl.CHECK_QUEUES) + "@" + getName ());
            LockSupport.unpark (this);
            return true;
        }
        return false;
    }

    // Unsafe mechanics
    private static final Unsafe U;
    private static final long WAKE_UP_TASK;
//    private static final long OFFS_STATUS;
    static {
        try {
            final Field f = Unsafe.class.getDeclaredField ("theUnsafe");
            f.setAccessible (true);
            U = (Unsafe) f.get (null);

            Class<?> k = WorkStealingThread.class;
            WAKE_UP_TASK = U.objectFieldOffset (k.getDeclaredField("wakeUpTask"));
//            OFFS_STATUS = U.objectFieldOffset (k.getDeclaredField ("status"));
        } catch (Exception e) {
            throw new Error(e);
        }
    }
}
