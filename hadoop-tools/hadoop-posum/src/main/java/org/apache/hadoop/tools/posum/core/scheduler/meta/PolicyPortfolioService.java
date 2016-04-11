package org.apache.hadoop.tools.posum.core.scheduler.meta;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.tools.posum.common.records.message.*;
import org.apache.hadoop.tools.posum.common.records.message.simple.SimpleRequest;
import org.apache.hadoop.tools.posum.common.records.message.simple.SimpleResponse;
import org.apache.hadoop.tools.posum.common.records.protocol.*;
import org.apache.hadoop.tools.posum.common.records.message.simple.impl.pb.ConfigurationRequestPBImpl;
import org.apache.hadoop.tools.posum.common.util.POSUMException;
import org.apache.hadoop.tools.posum.core.master.POSUMMasterContext;
import org.apache.hadoop.tools.posum.core.scheduler.portfolio.DataOrientedPolicy;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoResponse;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;

import java.io.IOException;

/**
 * Created by ane on 3/31/16.
 */
public class PolicyPortfolioService extends AbstractService implements PortfolioProtocol {

    private POSUMMasterContext pmContext;
    private Configuration posumConf;
    //TODO choose default some other way
    private Class<? extends PluginPolicy> currentSchedulerClass = DataOrientedPolicy.class;
    private PluginPolicy currentScheduler;

    public PolicyPortfolioService(POSUMMasterContext context) {
        super(PolicyPortfolioService.class.getName());
        this.pmContext = context;
        //TODO move somewhere else
        try {
            currentScheduler = currentSchedulerClass.newInstance();

        } catch (InstantiationException | IllegalAccessException e) {
            throw new POSUMException("Could not initialize plugin scheduler", e);
        }
    }

    @Override
    protected void serviceInit(Configuration conf) throws Exception {
        super.serviceInit(conf);
        this.posumConf = conf;
        currentScheduler.initializePlugin(posumConf);
    }

    public SimpleResponse forwardToScheduler(ConfigurationRequestPBImpl request) {
        try {
            Configuration yarnConf =
                    new DummyYarnConfiguration(request.getPayload());
            switch (request.getType()) {
                case CONFIG:
                    currentScheduler.setConf(yarnConf);
                    break;
                case INIT:
                    currentScheduler.init(yarnConf);
                    break;
                case REINIT:
                    RMContext rmContext = new DummyRMContext(posumConf, yarnConf);
                    currentScheduler.reinitialize(yarnConf, rmContext);
                    break;
                default:
                    return SimpleResponse.newInstance(false, "Could not recognize message type " + request.getType());
            }
        } catch (Exception e) {
            return SimpleResponse.newInstance(false, "Exception when forwarding message type " + request.getType(), e);
        }
        return SimpleResponse.newInstance(true);
    }

    @Override
    public SimpleResponse forwardToScheduler(SimpleRequest request) {
        try {
            switch (request.getType()) {
                case START:
                    currentScheduler.start();
                    break;
                case STOP:
                    currentScheduler.stop();
                    break;
                case NUM_NODES:
                    return SimpleResponse.newInstance(true, Integer.toString(currentScheduler.getNumClusterNodes()));
                default:
                    return SimpleResponse.newInstance(false, "Could not recognize message type " + request.getType());
            }
        } catch (Exception e) {
            return SimpleResponse.newInstance(false, "Exception when forwarding message type " + request.getType(), e);
        }
        return SimpleResponse.newInstance(true);
    }

    @Override
    public SimpleResponse handleSchedulerEvent(HandleSchedulerEventRequest request) {
        try {
            currentScheduler.handle(request.getInterpretedEvent());
        } catch (Exception e) {
            return SimpleResponse.newInstance(false, "Exception when handling event", e);
        }
        return SimpleResponse.newInstance(true);
    }

    @Override
    public SchedulerAllocateResponse allocateResources(SchedulerAllocateRequest request) {
        Allocation allocation = currentScheduler.allocate(
                request.getApplicationAttemptId(),
                request.getResourceRequests(),
                request.getReleases(),
                request.getBlacklistAdditions(),
                request.getBlacklistRemovals()
        );
        return SchedulerAllocateResponse.newInstance(allocation);
    }

    @Override
    public GetQueueInfoResponse getSchedulerQueueInfo(GetQueueInfoRequest request) {
        try {
            QueueInfo info = currentScheduler.getQueueInfo(
                    request.getQueueName(),
                    request.getIncludeChildQueues(),
                    request.getRecursive()
            );
            return GetQueueInfoResponse.newInstance(info);
        } catch (IOException e) {
            return GetQueueInfoResponse.newInstance(null);
        }

    }

}
