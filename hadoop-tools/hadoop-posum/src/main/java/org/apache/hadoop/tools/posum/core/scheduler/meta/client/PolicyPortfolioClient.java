package org.apache.hadoop.tools.posum.core.scheduler.meta.client;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.tools.posum.common.records.message.request.HandleSchedulerEventRequest;
import org.apache.hadoop.tools.posum.common.records.message.request.SchedulerAllocateRequest;
import org.apache.hadoop.tools.posum.common.records.message.request.SimpleRequest;
import org.apache.hadoop.tools.posum.common.records.message.reponse.SimpleResponse;
import org.apache.hadoop.tools.posum.common.records.protocol.*;
import org.apache.hadoop.tools.posum.common.util.POSUMConfiguration;
import org.apache.hadoop.tools.posum.common.util.POSUMException;
import org.apache.hadoop.tools.posum.common.util.StandardClientProxyFactory;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;

import java.io.IOException;
import java.util.*;

/**
 * Created by ane on 2/9/16.
 */
public class PolicyPortfolioClient extends AbstractService {

    private static Log logger = LogFactory.getLog(PolicyPortfolioClient.class);

    private Configuration posumConf;
    private Set<String> relevantProps;

    public PolicyPortfolioClient() {
        super(PolicyPortfolioClient.class.getName());
    }

    private POSUMMasterProtocol pmClient;

    @Override
    protected void serviceInit(Configuration conf) throws Exception {
        super.serviceInit(conf);
        this.posumConf = conf;
        relevantProps = new HashSet<>();
        String[] propStrings =
                posumConf.get(POSUMConfiguration.RELEVANT_SCHEDULER_CONFIGS,
                        POSUMConfiguration.DEFAULT_RELEVANT_SCHEDULER_CONFIGS).split(",");
        for (String propString : propStrings) {
            relevantProps.add(propString.trim());
        }
    }

    @Override
    protected void serviceStart() throws Exception {
        final Configuration conf = getConfig();
        try {
            pmClient = new StandardClientProxyFactory<>(conf, POSUMMasterProtocol.class).createProxy();
        } catch (IOException e) {
            throw new POSUMException("Could not init POSUMMaster client", e);
        }
        super.serviceStart();
    }

    @Override
    protected void serviceStop() throws Exception {
        if (this.pmClient != null) {
            RPC.stopProxy(this.pmClient);
        }
        super.serviceStop();
    }

    private <T> SimpleResponse<T> handleError(String type, SimpleResponse<T> response) {
        if (!response.getSuccessful()) {
            throw new POSUMException("Request type " + type + " returned with error: " + "\n" + response.getText(),
                    response.getException());
        }
        return response;
    }

    private SimpleResponse sendConfRequest(SimpleRequest.Type type, Configuration conf) {
        Map<String, String> properties = new HashMap<>();
        for (String prop : relevantProps) {
            properties.put(prop, conf.get(prop));
        }
        return handleError(type.name(), pmClient.forwardToScheduler(
                SimpleRequest.newInstance(type, properties)));
    }

    private SimpleResponse sendSimpleRequest(SimpleRequest.Type type) {
        return sendSimpleRequest(type, SimpleRequest.newInstance(type));
    }

    private SimpleResponse sendSimpleRequest(SimpleRequest.Type type, SimpleRequest request) {
        return handleError(type.name(), pmClient.forwardToScheduler(request));
    }

    public void setConf(Configuration conf) {
        sendConfRequest(SimpleRequest.Type.CONFIG, conf);
    }

    public void reinitScheduler(Configuration conf) {
        sendConfRequest(SimpleRequest.Type.REINIT, conf);

    }

    public void initScheduler(Configuration conf) {
        sendConfRequest(SimpleRequest.Type.INIT, conf);
    }

    public void startScheduler() {
        sendSimpleRequest(SimpleRequest.Type.START);
    }

    public void stopScheduler() {
        sendSimpleRequest(SimpleRequest.Type.STOP);
    }

    public void handleSchedulerEvent(SchedulerEvent event) {
        HandleSchedulerEventRequest request = HandleSchedulerEventRequest.newInstance(event);
        handleError("handleSchedulerEvent", pmClient.handleSchedulerEvent(request));
    }

    public Allocation allocateResources(ApplicationAttemptId applicationAttemptId,
                                        List<ResourceRequest> ask,
                                        List<ContainerId> release,
                                        List<String> blacklistAdditions,
                                        List<String> blacklistRemovals) {
        return handleError("allocateResources", pmClient.allocateResources(SchedulerAllocateRequest.newInstance(
                applicationAttemptId,
                ask,
                release,
                blacklistAdditions,
                blacklistRemovals))).getPayload();
    }

    public QueueInfo getSchedulerQueueInfo(String queueName, boolean includeApplications,
                                           boolean includeChildQueues, boolean recursive) {
        return handleError("getSchedulerQueueInfo", pmClient.getSchedulerQueueInfo(GetQueueInfoRequest.newInstance(
                queueName,
                includeApplications,
                includeChildQueues,
                recursive))).getPayload();
    }

    public int getNumClusterNodes() {
        return Integer.valueOf(sendSimpleRequest(SimpleRequest.Type.NUM_NODES).getText());
    }
}
