package org.apache.hadoop.tools.posum.test;

/**
 * Created by ane on 7/28/16.
 */
public interface TraceSnapshotMockDS extends MockDataStore {

    Long getSnapshotTime();

    Long setSnapshotTime();

    Long setSnapshotOffset();

    Long getTraceStartTime();

    Long getTraceFinishTime();
}
