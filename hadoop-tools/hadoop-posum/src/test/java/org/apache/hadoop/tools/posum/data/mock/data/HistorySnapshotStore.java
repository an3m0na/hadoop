package org.apache.hadoop.tools.posum.data.mock.data;

import org.apache.hadoop.tools.posum.client.data.DataStore;

public interface HistorySnapshotStore extends DataStore {

  Long getSnapshotTime();

  void setSnapshotTime(Long time);

  Long getSnapshotOffset();

  void setSnapshotOffset(Long offset);

  Long getTraceStartTime();

  Long getTraceFinishTime();
}
