package org.apache.hadoop.tools.posum.database.mock;

import org.apache.hadoop.tools.posum.database.client.DataBroker;

/**
 * Created by ane on 7/28/16.
 */
public interface HistorySnapshotBroker extends DataBroker {

    Long getSnapshotTime();

    void setSnapshotTime(Long time);

    Long getSnapshotOffset();

    void setSnapshotOffset(Long offset);

    Long getTraceStartTime();

    Long getTraceFinishTime();
}
