package org.apache.hadoop.tools.posum.common.records.protocol;

import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoResponse;

/**
 * Created by ane on 3/31/16.
 */
public interface PortfolioProtocol {
    long versionID = 1L;

    SimpleResponse forwardToScheduler(SimpleRequest request);
    SimpleResponse handleSchedulerEvent(HandleSchedulerEventRequest request);
    SchedulerAllocateResponse allocateResources(SchedulerAllocateRequest request);
    GetQueueInfoResponse getSchedulerQueueInfo(GetQueueInfoRequest request);
}
