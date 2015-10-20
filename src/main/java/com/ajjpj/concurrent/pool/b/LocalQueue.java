package com.ajjpj.concurrent.pool.b;

import com.ajjpj.afoundation.util.AUnchecker;
import sun.misc.Unsafe;

import java.lang.reflect.Field;


/**
 * @author arno
 */
class LocalQueue {
    /**
     * an array holding all currently submitted tasks.
     */
    private final AThreadPoolTask[] tasks;
    final WorkerThread thread = null;

    //TODO here and elsewhere: memory layout
    /**
     * a bit mask to project an offset into the valid range of offsets for the tasks array
     */
    private final int mask;

    final AThreadPoolImpl pool;

    private long base = 0;
    private long top = 0;

    /**
     * This variable serves solely to provide memory barriers - with release of Java 9 more explicit solutions will hopefully be available
     */
    private volatile int v=0;

    LocalQueue (AThreadPoolImpl pool, int size) {
        this.pool = pool;

        if (1 != Integer.bitCount (size)) throw new IllegalArgumentException ("size must be a power of 2");
        if (size < 8 || size > 1024*1024) throw new IllegalArgumentException ("size must be in the range from 8 to " + (1024*1024));

        this.tasks = new AThreadPoolTask[size];
        this.mask = size-1;
    }

    void init(WorkerThread thread) {
        AUnchecker.executeUnchecked (() -> {
            final Field field = LocalQueue.class.getDeclaredField ("thread");
            field.setAccessible (true);
            field.set (this, thread);
        });
    }

    /**
     * Add a new task to the top of the localQueue, incrementing 'top'. This is only ever called from the owning thread.
     */
    void push (AThreadPoolTask task) {
        final long _base = base; // read base first (and only once)
        final long _top = top;
        if (_top == _base + mask) {
            //TODO handle overflow
            throw new ArrayIndexOutOfBoundsException ("local localQueue overflow");
        }

        tasks[asArrayindex (_top)] = task;
        top = _top+1; // 'top' is only ever modified by the owning thread, so we need no CAS here
        v = 0;        // issue a store barrier

        // Notify pool only for the first added item per queue. This unparks *all* idling threads, which then look for work in all queues. So if this queue already
        //  contained work, either all workers are busy, or they are in the process of looking for work and will find this newly added item anyway without being notified
        //  again. //TODO does this require newly woken-up threads to scan twice? Is there still a race here?
        if (_top - _base <= 1) { //TODO take a closer look at this
            pool.onStealableTask ();
        }
    }

    /**
     * Fetch (and remove) a task from the top of the queue, i.e. LIFO semantics. This is only ever called from the owning thread, removing (or
     *  at least reducing) contention at the top of the queue: No other thread operates there.
     */
    AThreadPoolTask popLifo () {
        final long _top = top;
        final AThreadPoolTask result = tasks[asArrayindex (_top-1)];
        if (result == null) {
            // The queue is empty. It is possible for the queue to be empty even if the previous unprotected read does not return null, but
            //  it will only ever return null if the queue really is empty: New entries are only added by the owning thread, and this method
            //  'popLifo()' is also only ever called by the owning thread.
            return null;
        }

        if (! UNSAFE.compareAndSwapObject (this, taskOffset (_top-1), result, null)) {
            // The CAS operation failing means that another thread pulled the top-most item from the queue, so the queue is now definitely
            //  empty. It also null'ed out the task in the array if it was previously available, allowing to to be GC'ed when processing is
            //  finished.
            return null;
        }

        // Since 'result' is not null, and was not previously consumed by another thread, we can safely consume it --> decrement 'top'
        top = _top-1;
        return result;
    }

    /**
     * Fetch (and remove) a task from the bottom of the queue, i.e. FIFO semantics. This method can be called by any thread.
     */
    AThreadPoolTask popFifo () {
        long _base, _top;

        while (true) {
            int i = v; // issue a load barrier
            _base = base;
            _top = top;

            if (_base == _top) {
                // Terminate the loop: the queue is empty.
                //TODO verify that Hotspot optimizes this kind of return-from-the-middle well
                return null;
            }

            final AThreadPoolTask result = tasks[asArrayindex (_base)];
            // 'null' means that another thread concurrently fetched the task from under our nose. CAS ensures that only one thread
            //  gets the task, and allows GC when processing is finished
            if (result != null && UNSAFE.compareAndSwapObject (this, taskOffset (_base), result, null)) {
                base = _base+1;
                v=0; // issue a store barrier
                return result;
            }
        }
    }

    private long taskOffset (long l) {
        return OFFS_TASKS + SCALE_TASKS * (l & mask);
    }

    private int asArrayindex (long l) {
        return (int) (l & mask);
    }

    //------------- Unsafe stuff
    private static final Unsafe UNSAFE;

    private static final long OFFS_TASKS;
    private static final long SCALE_TASKS;

    static {
        try {
            final Field f = Unsafe.class.getDeclaredField ("theUnsafe");
            f.setAccessible (true);
            UNSAFE = (Unsafe) f.get (null);

            OFFS_TASKS = UNSAFE.arrayBaseOffset (AThreadPoolTask[].class);
            SCALE_TASKS = UNSAFE.arrayIndexScale (AThreadPoolTask[].class);
        }
        catch (Exception e) {
            AUnchecker.throwUnchecked (e);
            throw new RuntimeException(); // for the compiler
        }
    }
}
