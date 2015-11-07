package com.ajjpj.concurrent.pool.b;

import java.text.NumberFormat;


/**
 * @author arno
 */
public class AWorkerThreadStatistics {
    public final long numTasksExecuted;
    public final long numSharedTasksExecuted;
    public final long numSteals;
    public final long numExceptions;

    public final long numParks;
    public final long numFalseAlarmUnparks;
    public final long numSharedQueueSwitches;

    public final long numLocalSubmits;

    public final int approximateLocalQueueSize;

    public AWorkerThreadStatistics (long numTasksExecuted, long numSharedTasksExecuted, long numSteals, long numExceptions, long numParks, long numFalseAlarmUnparks, long numSharedQueueSwitches, long numLocalSubmits,
                                    int approximateLocalQueueSize) {
        this.numTasksExecuted = numTasksExecuted;
        this.numSharedTasksExecuted = numSharedTasksExecuted;
        this.numSteals = numSteals;
        this.numExceptions = numExceptions;
        this.numParks = numParks;
        this.numFalseAlarmUnparks = numFalseAlarmUnparks;
        this.numSharedQueueSwitches = numSharedQueueSwitches;
        this.numLocalSubmits = numLocalSubmits;
        this.approximateLocalQueueSize = approximateLocalQueueSize;
    }

    @Override public String toString () {
        return "WorkerThreadStatistics{" +
                "numTasksExecuted=" + NumberFormat.getNumberInstance ().format (numTasksExecuted) +
                ", numSharedTasksExecuted=" + NumberFormat.getNumberInstance ().format (numSharedTasksExecuted) +
                ", numSteals=" + NumberFormat.getNumberInstance ().format (numSteals) +
                ", numExceptions=" + NumberFormat.getNumberInstance ().format (numExceptions) +
                ", numParks=" + NumberFormat.getNumberInstance ().format (numParks) +
                ", numFalseAlarmUnparks=" + NumberFormat.getNumberInstance ().format (numFalseAlarmUnparks) +
                ", numSharedQueueSwitches=" + NumberFormat.getNumberInstance ().format (numSharedQueueSwitches) +
                ", numLocalSubmits=" + NumberFormat.getNumberInstance ().format (numLocalSubmits) +
                ", approximateLocalQueueSize=" + NumberFormat.getNumberInstance ().format (approximateLocalQueueSize) +
                '}';
    }
}
