package com.ajjpj.concurrent.pool.impl;

import com.ajjpj.afoundation.util.AUnchecker;
import com.ajjpj.concurrent.pool.api.exc.RejectedExecutionExceptionWithoutStacktrace;
import sun.misc.Unsafe;

import java.lang.reflect.Field;


/**
 * @author arno
 */
class SharedQueueNonBlockingImpl implements ASharedQueue {
    /**
     * an array holding all currently submitted tasks.
     */
    private final Runnable[] tasks;

    //TODO here and elsewhere: memory layout
    /**
     * a bit mask to project an offset into the valid range of offsets for the tasks array
     */
    private final int mask;

    final AThreadPoolImpl pool;

    long base = 0;
    long top = 0;

    SharedQueueNonBlockingImpl (AThreadPoolImpl pool, int size) {
        this.pool = pool;

        if (1 != Integer.bitCount (size)) throw new IllegalArgumentException ("size must be a power of 2");
        if (size < 8 || size > 1024*1024) throw new IllegalArgumentException ("size must be in the range from 8 to " + (1024*1024));

        this.tasks = new Runnable[size];
        this.mask = size-1;
    }

    /**
     * @return an approximation of the queue's current size. The value may be stale and is not synchronized in any way, and it is intended for debugging and statistics
     *  purposes only.
     */
    @Override public int approximateSize () {
        return (int) (
                UNSAFE.getLongVolatile (this, OFFS_TOP) -
                UNSAFE.getLongVolatile (this, OFFS_BASE)
        );
    }

    /**
     * Add a new task to the top of the shared queue, incrementing 'top'.
     */
    @Override public void push (Runnable task) {
        while (true) {
            final long _base = UNSAFE.getLongVolatile (this, OFFS_BASE);
            final long _top = top;

            if (_top == _base + mask) {
                throw new RejectedExecutionExceptionWithoutStacktrace ("Queue overflow");
            }

            final long taskOffset = taskOffset (_top);
            if (UNSAFE.compareAndSwapObject (tasks, taskOffset, null, task)) {
                // if the publishing thread is interrupted here, other publishers will effectively do a spin wait
                if (!UNSAFE.compareAndSwapLong (this, OFFS_TOP, _top, _top+1)) {
                    // there was a buffer wrap-around in the meantime --> undo the CAS 'put' operation and try again
                    UNSAFE.putObjectVolatile (tasks, taskOffset, null);
                    continue;
                }

                if (_top - _base <= 1) { //TODO take a closer look at this
                    pool.onAvailableTask ();
                }
                break;
            }
        }
    }

    @Override public synchronized Runnable popFifo () {
        final long _base = base;
        final long _top = top;

        if (_base == _top) {
            // Terminate the loop: the queue is empty.
            //TODO verify that Hotspot optimizes this kind of return-from-the-middle well
            return null;
        }

        final int arrIdx = asArrayIndex (_base);
        final Runnable result = tasks [arrIdx];
        if (result == null) return null; //TODO is this necessary?

        tasks[arrIdx] = null;

        // volatile put for atomicity and to ensure ordering wrt. nulling the task
        UNSAFE.putLongVolatile (this, OFFS_BASE, _base+1);

        return result;
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

    private static final long OFFS_BASE;
    private static final long OFFS_TOP;

    static {
        try {
            final Field f = Unsafe.class.getDeclaredField ("theUnsafe");
            f.setAccessible (true);
            UNSAFE = (Unsafe) f.get (null);

            OFFS_TASKS = UNSAFE.arrayBaseOffset (Runnable[].class);
            SCALE_TASKS = UNSAFE.arrayIndexScale (Runnable[].class);

            OFFS_BASE = UNSAFE.objectFieldOffset (SharedQueueNonBlockingImpl.class.getDeclaredField ("base"));
            OFFS_TOP  = UNSAFE.objectFieldOffset (SharedQueueNonBlockingImpl.class.getDeclaredField ("top"));
        }
        catch (Exception e) {
            AUnchecker.throwUnchecked (e);
            throw new RuntimeException(); // for the compiler
        }
    }
}
