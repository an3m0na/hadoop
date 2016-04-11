package org.apache.hadoop.tools.posum.common.records.protocol;

import org.apache.hadoop.tools.posum.common.records.message.request.HandleRMEventRequest;
import org.apache.hadoop.tools.posum.common.records.message.reponse.SimpleResponse;

/**
 * Created by ane on 3/31/16.
 */
public interface MetaSchedulerProtocol {
    long versionID = 1L;

    SimpleResponse handleRMEvent(HandleRMEventRequest request);
}
