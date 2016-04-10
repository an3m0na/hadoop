package org.apache.hadoop.tools.posum.core.scheduler.meta;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.tools.posum.common.records.protocol.*;
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
            this.currentScheduler = currentSchedulerClass.newInstance();

        } catch (InstantiationException | IllegalAccessException e) {
            throw new POSUMException("Could not initialize plugin scheduler", e);
        }
    }

    @Override
    protected void serviceInit(Configuration conf) throws Exception {
        super.serviceInit(conf);
        this.posumConf = conf;
        this.currentScheduler.initializePlugin(posumConf);
    }

    @Override
    public SimpleResponse configureScheduler(ConfigurationRequest request) {
        this.currentScheduler.setConf(new DummyYarnConfiguration(request.getProperties()));
        return SimpleResponse.newInstance(true);
    }

    @Override
    public SimpleResponse initScheduler(ConfigurationRequest request) {
        this.currentScheduler.init(new DummyYarnConfiguration(request.getProperties()));
        return SimpleResponse.newInstance(true);
    }

    @Override
    public SimpleResponse reinitScheduler(ConfigurationRequest request) {
        try {
            Configuration yarnConf = new DummyYarnConfiguration(request.getProperties());
            RMContext rmContext = new DummyRMContext(posumConf, yarnConf);

            this.currentScheduler.reinitialize(yarnConf, rmContext);
        } catch (IOException e) {
            return SimpleResponse.newInstance(false, "Reinitialization exception", e);
        }
        return SimpleResponse.newInstance(true);
    }

    @Override
    public SimpleResponse handleSchedulerEvent(HandleSchedulerEventRequest request) {
        //TODO handle dispatch response
        try {
            this.currentScheduler.handle(request.getInterpretedEvent());
        } catch (Exception e) {
            return SimpleResponse.newInstance(false, "Exception in handling event", e);
        }
        return SimpleResponse.newInstance(true);
    }

    @Override
    public SchedulerAllocateResponse allocateResources(SchedulerAllocateRequest request) {
        Allocation allocation = this.currentScheduler.allocate(
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
            QueueInfo info = this.currentScheduler.getQueueInfo(
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
