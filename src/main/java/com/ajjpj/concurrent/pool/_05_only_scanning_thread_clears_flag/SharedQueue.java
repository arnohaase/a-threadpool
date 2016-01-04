package com.ajjpj.concurrent.pool._05_only_scanning_thread_clears_flag;

import com.ajjpj.afoundation.util.AUnchecker;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


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

    final Map<Long, String> byTopValue = new ConcurrentHashMap<> ();

    /**
     * @return an approximation of the queue's current size. The value may be stale and is not synchronized in any way, and it is intended for debugging and statistics
     *  purposes only.
     */
    int approximateSize () {
        return (int) (
                UNSAFE.getLongVolatile (this, OFFS_TOP) -
                UNSAFE.getLongVolatile (this, OFFS_BASE)
        );
    }

    /**
     * Add a new task to the top of the localQueue, incrementing 'top'.
     */
    void push (AThreadPoolTask task) {
        lock ();

        final long _base;
        final long _top;
        try {
            _base = UNSAFE.getLongVolatile (this, OFFS_BASE);
            _top = UNSAFE.getLongVolatile (this, OFFS_TOP);

            if (_top == _base + mask) {
                throw new ArrayIndexOutOfBoundsException ("Queue overflow"); //TODO create back pressure instead; --> how to handle with 'overflow' submissions from worker threads?!
            }

            // We hold a lock here, so there can be no concurrent modifications of 'top', and there is no need for CAS. We must however ensure that
            //  no concurrently reading thread sees the incremented 'top' without the task being present in the array, therefore the 'ordered' put.
            UNSAFE.putObjectVolatile (tasks, taskOffset (_top), task);
            UNSAFE.putLongVolatile (this, OFFS_TOP, _top+1); //TODO volatile should not be necessary 'top' is published by 'unlock()' below.
        }
        finally {
            unlock ();
        }

        // Notify pool only for the first added item per queue. This unparks *all* idling threads, which then look for work in all queues. So if this queue already
        //  contained work, either all workers are busy, or they are in the process of looking for work and will find this newly added item anyway without being notified
        //  again. //TODO does this require newly woken-up threads to scan twice? Is there still a race here?
        if (_top - _base <= 1) { //TODO take a closer look at this
            pool.onAvailableTask ();
        }
    }

    //TODO this is a (more or less) lock free implementation --> benchmark this with several publishers
//    void push2 (AThreadPoolTask task) {
//        while (true) {
//            final long _base = UNSAFE.getLongVolatile (this, OFFS_BASE);
//            final long _top = top;
//
//            if (_top == _base + mask) {
//                throw new ArrayIndexOutOfBoundsException ("Queue overflow"); //TODO create back pressure instead; --> how to handle with 'overflow' submissions from worker threads?!
//            }
//
//            if (UNSAFE.compareAndSwapObject (tasks, taskOffset (_top), null, task)) {
//                // if the publishing thread is interrupted here, other publishers will effectively do a spin wait
//                UNSAFE.putOrderedLong (this, OFFS_TOP, _top+1); //TODO if we use CAS here, we can let other threads increment 'top' until they find a free slot
//
//                if (_top - _base <= 1) { //TODO take a closer look at this
//                    pool.onAvailableTask ();
//                }
//                break;
//            }
//        }
//    }

    final Map<Long, String> baseValues = new ConcurrentHashMap<> ();
    final Map<Long, Long> offsets = new ConcurrentHashMap<> ();

    /**
     * Fetch (and remove) a task from the bottom of the queue, i.e. FIFO semantics. This method can be called by any thread.
     */
    AThreadPoolTask popFifo () {
        int counter = 0;

        while (true) {
            final long _base = UNSAFE.getLongVolatile (this, OFFS_BASE);
            final long _top = UNSAFE.getLongVolatile (this, OFFS_TOP);

            counter += 1;

            if (_base == _top) {
                // Terminate the loop: the queue is empty.
                //TODO verify that Hotspot optimizes this kind of return-from-the-middle well
                return null;
            }

            final AThreadPoolTask result = tasks[asArrayIndex (_base)];


            if (_top > _base && UNSAFE.compareAndSwapLong (this, OFFS_BASE, _base, _base+1)) {
                return (AThreadPoolTask) UNSAFE.getAndSetObject (tasks, taskOffset (_base), null);
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
    private static final long OFFS_BASE;
    private static final long OFFS_TOP;

    static {
        try {
            final Field f = Unsafe.class.getDeclaredField ("theUnsafe");
            f.setAccessible (true);
            UNSAFE = (Unsafe) f.get (null);

            OFFS_TASKS = UNSAFE.arrayBaseOffset (AThreadPoolTask[].class);
            SCALE_TASKS = UNSAFE.arrayIndexScale (AThreadPoolTask[].class);

            OFFS_LOCK = UNSAFE.objectFieldOffset (SharedQueue.class.getDeclaredField ("lock"));
            OFFS_BASE = UNSAFE.objectFieldOffset (SharedQueue.class.getDeclaredField ("base"));
            OFFS_TOP  = UNSAFE.objectFieldOffset (SharedQueue.class.getDeclaredField ("top"));
        }
        catch (Exception e) {
            AUnchecker.throwUnchecked (e);
            throw new RuntimeException(); // for the compiler
        }
    }
}
