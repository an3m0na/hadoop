package org.apache.hadoop.tools.posum.database.mock;

import org.apache.hadoop.tools.posum.database.client.DataBroker;

/**
 * Created by ane on 7/28/16.
 */
public interface TraceSnapshotMockDS extends DataBroker {

    Long getSnapshotTime();

    Long setSnapshotTime();

    Long setSnapshotOffset();

    Long getTraceStartTime();

    Long getTraceFinishTime();
}
