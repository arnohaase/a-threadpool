package com.ajjpj.concurrent.pool.a;

import com.ajjpj.concurrent.pool.a.WorkStealingPoolImpl.ASubmittable;
import sun.misc.Contended;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.concurrent.RejectedExecutionException;


/**
 * @author arno
 */
@Contended
class WorkStealingGlobalQueue {
    public static final int MAX_CAPACITY = 1 << 26;

    @SuppressWarnings ("unused") // all access goes through Unsafe
    private int qlock;          // 1: locked, else 0

    private volatile int isShutdown = 0;

    private volatile long base;           // index of next slot for poll - never wraps, filtered with bit mask instead
    private long top;                     // index of next slot for push - never wraps, filtered with bit mask instead

    final int mask;             // bit mask for accessing the element array
    final ASubmittable[] array; // the elements


    WorkStealingGlobalQueue (int capacity) {
        if (Integer.bitCount (capacity) != 1) {
            throw new IllegalArgumentException ("capacity must be a power of two, is " + capacity);
        }
        if (capacity < 8) {
            throw new IllegalArgumentException ("capacity must be at least 8, is " + capacity);
        }
        if (capacity > MAX_CAPACITY) {
            throw new IllegalArgumentException ("capacity must not be bigger than " + MAX_CAPACITY + ", is " + capacity);
        }

        // Place indices in the center of array (that is not yet allocated)
        base = top = capacity / 2;
        array = new ASubmittable[capacity];
        mask = capacity-1;
    }

    private long getBase() {
        checkShutdown ();
        return base;
    }

    private void checkShutdown() {
        if (isShutdown == 1) {
            throw new WorkStealingShutdownException ();
        }
    }

    void shutdown() {
        isShutdown = 1;
    }


//    boolean tryWakeupWorker (ASubmittable task) {
//
//        WorkStealingThread availableWorker = null;
//
//        synchronized (this) {
////        //noinspection StatementWithEmptyBody
////        while (! U.compareAndSwapInt (this, QLOCK, 0, 1)) { }
////
////        try {
//            availableWorker = doTryWakeUpWorker ();
////        }
////        finally {
////            qlock = 0;
////        }
//        }
//
//        if (availableWorker != null) {
//            availableWorker.wakeUpWith (task);
//            return true;
//        }
//        return false;
//    }

    /**
     * Does the actual waking up of a thread, if available. This method must only be called with the 'qlock' spin lock held.
     */
//    private WorkStealingThread _doTryWakeUpWorker () {
//        // shortcut: if threads are waiting, wake one of them up with the new task rather than going through the queue proper.
//
//        if (waitingWorkers.isEmpty ()) {
//            return null;
//        }
//        // in the other path, 'getBase()' is called and checks for shutdown.
//        checkShutdown ();
//
//        final WorkStealingThread availableWorker = waitingWorkers.head ();
//        waitingWorkers = waitingWorkers.tail ();
//        return availableWorker;
//    }

    /**
     * This is the only place 'top' is modified, and since that happens in a lock, there is no need for protection against races.
     */
    final void submit (ASubmittable task) {
        if (task == null) {
            throw new IllegalArgumentException ();
        }

        synchronized (this) {

//            //noinspection StatementWithEmptyBody
//            while (!U.compareAndSwapInt (this, QLOCK, 0, 1)) {
//                // acquire spin lock
//            }


//            try {
                // shortcut: if threads are waiting, wake one of them up with the new task rather than going through the queue proper.
                    final long n = top - getBase (); // call 'getBase()' early to ensure

                    if (n >= mask) {
                        throw new RejectedExecutionException ();//TODO message
                    }

                    final long j = unsafeArrayOffset (top);

                    //TODO Verify that we can get away with a regular mutable field for the queue index so as to avoid object creation with associated barriers - all read access goes
                    //TODO  through a U.getObjectVolatile, so we should be fine, right? The difference is in the implementation of 'withQueueIndex'
                    U.putOrderedObject (array, j, task.withQueueIndex (top));
                    top += 1;
                    return;
                }
//            }
//            finally {
//                //TODO verify that this is a valid optimization
////            U.putOrderedInt (this, QLOCK, 0);
//                qlock = 0;
//            }
    }

    /**
     * Takes next task, if one exists, in FIFO order.
     */
    final ASubmittable poll() {
        long b;

        while ((b = getBase ()) < top) {
            final long j = unsafeArrayOffset (b);
            final ASubmittable t = (ASubmittable) U.getObjectVolatile (array, j);

            if (t != null) {
                if (t.queueIndex != b) {
                    // This check ensures that we actually fetched the task at 'b' rather than the next
                    //  task after the ring buffer wrapped around.
                    continue;
                }

                if (U.compareAndSwapObject (array, j, t, null)) {
                    U.putOrderedLong (this, QBASE, b + 1);
                    return t;
                }
            }
            else {
                if (b + 1 == top) {
                    // another thread is currently removing the last entry --> short cut
                    break;
                }

                Thread.yield(); // wait for lagging update (very rare)
            }
        }
        return null;
    }

    // Unsafe mechanics

    private long unsafeArrayOffset (long index) {
        return ((mask & index) << ASHIFT) + ABASE;
    }

    private static final Unsafe U;
    private static final long QBASE;
    private static final long QLOCK;
    private static final long QSHUTDOWN;
    private static final int ABASE;
    private static final int ASHIFT;
    static {
        try {
            final Field f = Unsafe.class.getDeclaredField ("theUnsafe");
            f.setAccessible (true);
            U = (Unsafe) f.get (null);


            Class<?> k = WorkStealingGlobalQueue.class;
            Class<?> ak = WorkStealingGlobalQueue[].class;
            QBASE     = U.objectFieldOffset (k.getDeclaredField("base"));
            QLOCK     = U.objectFieldOffset (k.getDeclaredField("qlock"));
            QSHUTDOWN = U.objectFieldOffset (k.getDeclaredField("isShutdown"));
            ABASE = U.arrayBaseOffset (ak);
            final int scale = U.arrayIndexScale(ak);
            if ((scale & (scale - 1)) != 0) {
                throw new Error ("data type scale not a power of two");
            }
            ASHIFT = 31 - Integer.numberOfLeadingZeros(scale);
        } catch (Exception e) {
            throw new Error(e);
        }
    }
}
