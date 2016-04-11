package org.apache.hadoop.tools.posum.common.records.protocol;

import org.apache.hadoop.tools.posum.common.records.message.request.HandleSchedulerEventRequest;
import org.apache.hadoop.tools.posum.common.records.message.request.SchedulerAllocateRequest;
import org.apache.hadoop.tools.posum.common.records.message.request.SimpleRequest;
import org.apache.hadoop.tools.posum.common.records.message.reponse.SimpleResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoRequest;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;

/**
 * Created by ane on 3/31/16.
 */
public interface PortfolioProtocol {
    long versionID = 1L;

    SimpleResponse forwardToScheduler(SimpleRequest request);
    SimpleResponse handleSchedulerEvent(HandleSchedulerEventRequest request);
    SimpleResponse<Allocation> allocateResources(SchedulerAllocateRequest request);
    SimpleResponse<QueueInfo> getSchedulerQueueInfo(GetQueueInfoRequest request);
}
