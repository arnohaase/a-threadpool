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
class WorkStealingLocalQueue {
    public static final int MAX_CAPACITY = 1 << 26;

    final boolean lifo; // mode for local access

    @SuppressWarnings ("FieldCanBeLocal") // write access goes through Unsafe
    private volatile int isShutdown = 0;

    private volatile long base;                  // index of next slot for poll
    private long top;                            // index of next slot for push

    private final int mask;             // bit mask for accessing elements of the array
    private final ASubmittable[] array; // the elements

    WorkStealingLocalQueue (int capacity, boolean lifo) {
        this.lifo = lifo;

        // Place indices in the center of array
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

    void checkShutdown () {
        if (isShutdown == 1) {
            throw new WorkStealingShutdownException ();
        }
    }

    void shutdown() {
        isShutdown = 1;
    }

    /**
     * Submits a task. Call only by owner.
     *
     * @throws java.util.concurrent.RejectedExecutionException if queue is full
     */
    final void submit (ASubmittable task) {
        if (task == null) {
            throw new NullPointerException ();
        }

        final long n = top - getBase ();
        if (n >= mask) {
            throw new RejectedExecutionException ();
        }

        // 'top' is only ever modified from the owning thread, so there is no need for guarding it.
        U.putOrderedObject (array, unsafeArrayOffset (top), task.withQueueIndex (top));
        U.putOrderedLong (this, QTOP, top + 1); //TODO is this necessary? F/J uses regular writes...
    }


    /**
     * Takes next task, if one exists, in LIFO order.  Called only by owning thread.
     */
    private final ASubmittable pop() {
        // 'top' is only ever modified from the owning thread, so there is no need for guarding it or refreshing its value.
        final long newTop = top-1;

        // 'top' is only ever modified from the owning thread, so there is no need for guarding it.
        while (newTop >= getBase ()) {
            long j = unsafeArrayOffset (newTop);
            final ASubmittable t = (ASubmittable) U.getObject (array, j);
            // no need to guard against wrap-arounds: There can be no new submissions to the queue while this method is executing.
            if (t == null) {
                break;
            }
            if (U.compareAndSwapObject (array, j, t, null)) {
                U.putOrderedLong (this, QTOP, newTop); //TODO is this necessary? F/J uses regular writes...
                return t;
            }
        }
        return null;
    }

    /**
     * Takes next task, if one exists, in FIFO order. This is the only modifying method that may be called from other threads.
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
            else if (getBase () == b) {
                if (b + 1 == top) {
                    // another thread is currently removing the last entry --> short cut
                    break;
                }

                Thread.yield(); // wait for lagging update (very rare)
            }
        }
        return null;
    }

    /**
     * Takes next task, if one exists, in order specified by mode.
     */
    final ASubmittable nextLocalTask() {
        return lifo ? pop() : poll();
    }


    private long unsafeArrayOffset (long index) {
        return ((mask & index) << ASHIFT) + ABASE;
    }

    // Unsafe mechanics
    private static final Unsafe U;
    private static final long QBASE;
    private static final long QTOP;
    private static final int ABASE;
    private static final int ASHIFT;
    static {
        try {
            final Field f = Unsafe.class.getDeclaredField ("theUnsafe");
            f.setAccessible (true);
            U = (Unsafe) f.get (null);

            Class<?> k = WorkStealingLocalQueue.class;
            Class<?> ak = ASubmittable[].class;
            QBASE = U.objectFieldOffset (k.getDeclaredField ("base"));
            QTOP = U.objectFieldOffset (k.getDeclaredField ("top"));
            ABASE = U.arrayBaseOffset (ak);
            int scale = U.arrayIndexScale (ak);
            if ((scale & (scale - 1)) != 0) {
                throw new Error ("data type scale not a power of two");
            }
            ASHIFT = 31 - Integer.numberOfLeadingZeros(scale);
        } catch (Exception e) {
            throw new Error(e);
        }
    }
}
