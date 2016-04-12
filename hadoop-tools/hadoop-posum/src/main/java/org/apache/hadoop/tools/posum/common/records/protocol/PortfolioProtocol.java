package org.apache.hadoop.tools.posum.common.records.protocol;

import org.apache.hadoop.tools.posum.common.records.request.HandleSchedulerEventRequest;
import org.apache.hadoop.tools.posum.common.records.request.SchedulerAllocateRequest;
import org.apache.hadoop.tools.posum.common.records.request.SimpleRequest;
import org.apache.hadoop.tools.posum.common.records.reponse.SimpleResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoRequest;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;

import java.io.IOException;

/**
 * Created by ane on 3/31/16.
 */
public interface PortfolioProtocol {
    long versionID = 1L;

    SimpleResponse forwardToScheduler(SimpleRequest request) throws IOException, YarnException;
    SimpleResponse handleSchedulerEvent(HandleSchedulerEventRequest request) throws IOException, YarnException;
    SimpleResponse<Allocation> allocateResources(SchedulerAllocateRequest request) throws IOException, YarnException;
    SimpleResponse<QueueInfo> getSchedulerQueueInfo(GetQueueInfoRequest request) throws IOException, YarnException;
}
