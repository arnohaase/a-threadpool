package com.ajjpj.concurrent.pool.b;

import com.ajjpj.afoundation.util.AUnchecker;
import sun.misc.Unsafe;

import java.lang.reflect.Field;


/**
 * @author arno
 */
class SharedQueue {
    /**
     * an array holding all currently submitted tasks.
     */
    private final AThreadPoolTask[] tasks;

    //TODO here and elsewhere: memory layout
    /**
     * a bit mask to project an offset into the valid range of offsets for the tasks array
     */
    private final int mask;

    final AThreadPoolImpl pool;

    long base = 0;
    long top = 0;

    /**
     * This variable serves solely to provide memory barriers - with release of Java 9 more explicit solutions will hopefully be available
     */
    private volatile int v=0;

    /**
     * This variable serves as a spin lock. All access goes through UNSAFE.
     */
    @SuppressWarnings ("UnusedDeclaration")
    private int lock = 0;

    SharedQueue (AThreadPoolImpl pool, int size) {
        this.pool = pool;

        if (1 != Integer.bitCount (size)) throw new IllegalArgumentException ("size must be a power of 2");
        if (size < 8 || size > 1024*1024) throw new IllegalArgumentException ("size must be in the range from 8 to " + (1024*1024));

        this.tasks = new AThreadPoolTask[size];
        this.mask = size-1;
    }

    /**
     * Add a new task to the top of the localQueue, incrementing 'top'.
     */
    void push (AThreadPoolTask task) {
        lock ();

        try {
            final long _base = base; // read base first (and only once)
            final long _top = top;
            if (_top == _base + mask) {
                throw new ArrayIndexOutOfBoundsException ("Queue overflow"); //TODO create back pressure instead; --> how to handle with 'overflow' submissions from worker threads?!
            }

            // We hold a lock here, so there can be no concurrent modifications of 'top', and there is no need for CAS. We must however ensure that
            //  no concurrently reading thread sees the incremented 'top' without the task being present in the array, therefore the 'ordered' put.
            UNSAFE.putOrderedObject (tasks, taskOffset (_top), task);
            top = _top+1; // 'top' is published by 'unlock()' below.

            // Notify pool only for the first added item per queue. This unparks *all* idling threads, which then look for work in all queues. So if this queue already
            //  contained work, either all workers are busy, or they are in the process of looking for work and will find this newly added item anyway without being notified
            //  again. //TODO does this require newly woken-up threads to scan twice? Is there still a race here?
            if (_top - _base <= 1) { //TODO take a closer look at this
                pool.onStealableTask ();
            }
        }
        finally {
            unlock ();
        }
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

            final AThreadPoolTask result = tasks[asArrayIndex (_base)];

            // 'null' means that another thread concurrently fetched the task from under our nose. CAS ensures that only one thread
            //  gets the task, and allows GC when processing is finished
            if (result != null && UNSAFE.compareAndSwapObject (tasks, taskOffset (_base), result, null)) {
                base = _base + 1;
                v = 0; // issue a store barrier
                return result;
            }
        }
    }

    private void lock() {
        //noinspection StatementWithEmptyBody
        while (!tryLock ()); //TODO add 'pause' when that becomes available
    }

    private boolean tryLock() {
        return UNSAFE.compareAndSwapInt (this, OFFS_LOCK, 0, 1);
    }

    private void unlock() {
        UNSAFE.putIntVolatile (this, OFFS_LOCK, 0);
    }

    private long taskOffset (long l) {
        return OFFS_TASKS + SCALE_TASKS * (l & mask);
    }

    private int asArrayIndex (long l) {
        return (int) (l & mask);
    }

    //------------- Unsafe stuff
    private static final Unsafe UNSAFE;

    private static final long OFFS_TASKS;
    private static final long SCALE_TASKS;

    private static final long OFFS_LOCK;

    static {
        try {
            final Field f = Unsafe.class.getDeclaredField ("theUnsafe");
            f.setAccessible (true);
            UNSAFE = (Unsafe) f.get (null);

            OFFS_TASKS = UNSAFE.arrayBaseOffset (AThreadPoolTask[].class);
            SCALE_TASKS = UNSAFE.arrayIndexScale (AThreadPoolTask[].class);

            OFFS_LOCK = UNSAFE.objectFieldOffset (SharedQueue.class.getDeclaredField ("lock"));
        }
        catch (Exception e) {
            AUnchecker.throwUnchecked (e);
            throw new RuntimeException(); // for the compiler
        }
    }
}
