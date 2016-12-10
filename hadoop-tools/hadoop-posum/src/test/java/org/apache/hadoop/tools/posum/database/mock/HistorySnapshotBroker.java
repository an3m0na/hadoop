package org.apache.hadoop.tools.posum.database.mock;

import org.apache.hadoop.tools.posum.database.client.DataBroker;

public interface HistorySnapshotBroker extends DataBroker {

    Long getSnapshotTime();

    void setSnapshotTime(Long time);

    Long getSnapshotOffset();

    void setSnapshotOffset(Long offset);

    Long getTraceStartTime();

    Long getTraceFinishTime();
}
