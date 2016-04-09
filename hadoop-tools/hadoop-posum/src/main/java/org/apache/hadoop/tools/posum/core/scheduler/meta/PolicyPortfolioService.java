package org.apache.hadoop.tools.posum.core.scheduler.meta;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.tools.posum.common.records.protocol.*;
import org.apache.hadoop.tools.posum.common.util.POSUMException;
import org.apache.hadoop.tools.posum.core.master.POSUMMasterContext;
import org.apache.hadoop.tools.posum.core.scheduler.portfolio.DataOrientedPolicy;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetQueueInfoResponsePBImpl;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.*;

import java.io.IOException;

/**
 * Created by ane on 3/31/16.
 */
public class PolicyPortfolioService extends AbstractService implements PortfolioProtocol {

    POSUMMasterContext pmContext;
    Configuration conf;
    //TODO choose default some other way
    Class<? extends PluginPolicy> currentSchedulerClass = DataOrientedPolicy.class;
    PluginPolicy currentScheduler;

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
        this.conf = conf;
        this.currentScheduler.initializePlugin(conf);
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
            RMContext rmContext = new DummyRMContext(yarnConf);

            this.currentScheduler.reinitialize(yarnConf, rmContext);
        } catch (IOException e) {
            return SimpleResponse.newInstance(false, "Reinitialization exception", e);
        }
        return SimpleResponse.newInstance(true);
    }

    @Override
    public HandleEventResponse handleSchedulerEvent(HandleEventRequest request) {
        SchedulerEvent event = null;
        switch (request.getEventType()) {
            case NODE_ADDED:
                event = new NodeAddedSchedulerEvent(request.getNode(), request.getContainerReports());
                break;
            case NODE_REMOVED:
                event = new NodeRemovedSchedulerEvent(request.getNode());
                break;
            case NODE_UPDATE:
                event = new NodeUpdateSchedulerEvent(request.getNode());
                break;
            case NODE_RESOURCE_UPDATE:
                event = new NodeResourceUpdateSchedulerEvent(request.getNode(), request.getResourceOption());
                break;
            case NODE_LABELS_UPDATE:
                //TODO using NodeIdToLabelsProto because it is used by capacity scheduler
//                event = new NodeLabelsUpdateSchedulerEvent()
                break;
            case APP_ADDED:
                event = new AppAddedSchedulerEvent(
                        request.getApplicationId(),
                        request.getQueue(),
                        request.getUser(),
                        request.getFlag(),
                        request.getReservationId()
                );
                break;
            case APP_REMOVED:
                event = new AppRemovedSchedulerEvent(request.getApplicationId(),
                        RMAppState.valueOf(request.getFinalState()));
                break;
            case APP_ATTEMPT_ADDED:
                event = new AppAttemptAddedSchedulerEvent(
                        ApplicationAttemptId.newInstance(request.getApplicationId(), request.getAttemptId()),
                        request.getFlag()
                );
                break;
            case APP_ATTEMPT_REMOVED:
                event = new AppAttemptRemovedSchedulerEvent(
                        ApplicationAttemptId.newInstance(request.getApplicationId(), request.getAttemptId()),
                        RMAppAttemptState.valueOf(request.getFinalState()),
                        request.getFlag()
                );
                break;
            case CONTAINER_EXPIRED:
                ApplicationAttemptId attemptId =
                        ApplicationAttemptId.newInstance(request.getApplicationId(), request.getAttemptId());
                ContainerId id = ContainerId.newContainerId(attemptId, request.getContainerId());
                event = new ContainerExpiredSchedulerEvent(id);
                break;
            default:
                return HandleEventResponse.newInstance(false,
                        "Unrecognized event type reached POSUM Scheduler " + request.getEventType());
        }
        //TODO handle dispatch response
        try {
            this.currentScheduler.handle(event);
        } catch (Exception e) {
            return HandleEventResponse.newInstance(false, "Exception handling error", e);
        }
        return HandleEventResponse.newInstance(true);
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
