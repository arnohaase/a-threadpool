package com.ajjpj.concurrent.pool.api;

import java.util.Arrays;


/**
 * @author arno
 */
public class AThreadPoolStatistics {
    public final AWorkerThreadStatistics[] workerThreadStatistics;
    public final ASharedQueueStatistics[] sharedQueueStatisticses;

    public AThreadPoolStatistics (AWorkerThreadStatistics[] workerThreadStatistics, ASharedQueueStatistics[] sharedQueueStatisticses) {
        this.workerThreadStatistics = workerThreadStatistics;
        this.sharedQueueStatisticses = sharedQueueStatisticses;
    }

    @Override public String toString () {
        return "AThreadPoolStatistics{" +
                "workerThreadStatistics=" + Arrays.toString (workerThreadStatistics) +
                ", sharedQueueStatisticses=" + Arrays.toString (sharedQueueStatisticses) +
                '}';
    }
}
