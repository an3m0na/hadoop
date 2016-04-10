package org.apache.hadoop.tools.posum.core.scheduler.meta.client;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.tools.posum.common.records.protocol.*;
import org.apache.hadoop.tools.posum.common.records.protocol.impl.pb.ConfigurationRequestPBImpl;
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

    private void logIfError(SimpleResponse response, String message) {
        if (!response.getSuccessful()) {
            logger.error(message + "\n" + response.getText(), response.getException());
        }
    }

    private SimpleRequest composeConfRequest(SimpleRequest.Type type, Configuration conf) {
        Map<String, String> properties = new HashMap<>();
        for (String prop : relevantProps) {
            properties.put(prop, conf.get(prop));
        }
        try {
            return SimpleRequest.newInstance(type, properties, ConfigurationRequestPBImpl.class);
        } catch (IllegalAccessException | InstantiationException e) {
            throw new POSUMException("Could not instantiate request", e);
        }
    }

    public void setConf(Configuration conf) {
        logIfError(pmClient.forwardToScheduler(composeConfRequest(SimpleRequest.Type.CONFIG, conf)),
                "Configuration unsuccessful");
    }

    public void reinitScheduler(Configuration conf) {
        logIfError(pmClient.forwardToScheduler(composeConfRequest(SimpleRequest.Type.REINIT, conf)),
                "Reinitialization unsuccessful");

    }

    public void initScheduler(Configuration conf) {
        logIfError(pmClient.forwardToScheduler(composeConfRequest(SimpleRequest.Type.INIT, conf)),
                "Initialization unsuccessful");
    }

    public void startScheduler() {
        logIfError(pmClient.forwardToScheduler(SimpleRequest.newInstance(SimpleRequest.Type.START)),
                "Scheduler start unsuccessful");
    }

    public void stopScheduler() {
        logIfError(pmClient.forwardToScheduler(SimpleRequest.newInstance(SimpleRequest.Type.STOP)),
                "Scheduler start unsuccessful");
    }

    public void handleSchedulerEvent(SchedulerEvent event) {
        HandleSchedulerEventRequest request = HandleSchedulerEventRequest.newInstance(event);
        logIfError(pmClient.handleSchedulerEvent(request), "Event handling unsuccessful");
    }

    public Allocation allocateResources(ApplicationAttemptId applicationAttemptId,
                                        List<ResourceRequest> ask,
                                        List<ContainerId> release,
                                        List<String> blacklistAdditions,
                                        List<String> blacklistRemovals) {
        return pmClient.allocateResources(SchedulerAllocateRequest.newInstance(
                applicationAttemptId,
                ask,
                release,
                blacklistAdditions,
                blacklistRemovals)).getAllocation();
    }

    public QueueInfo getSchedulerQueueInfo(String queueName, boolean includeApplications,
                                           boolean includeChildQueues, boolean recursive) {
        return pmClient.getSchedulerQueueInfo(GetQueueInfoRequest.newInstance(
                queueName,
                includeApplications,
                includeChildQueues,
                recursive)
        ).getQueueInfo();
    }
}
