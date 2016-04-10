package org.apache.hadoop.tools.posum.common.records.protocol;

/**
 * Created by ane on 3/31/16.
 */
public interface MetaSchedulerProtocol {
    long versionID = 1L;

    SimpleResponse handleRMEvent(HandleRMEventRequest request);
}
