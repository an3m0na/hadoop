package org.apache.hadoop.tools.posum.test;

import org.apache.hadoop.tools.posum.database.client.ExtendedDataClientInterface;

/**
 * Created by ane on 7/28/16.
 */
public interface TraceSnapshotMockDS extends ExtendedDataClientInterface {

    Long getSnapshotTime();

    Long setSnapshotTime();

    Long setSnapshotOffset();

    Long getTraceStartTime();

    Long getTraceFinishTime();
}
