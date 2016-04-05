package org.apache.hadoop.tools.posum.core.scheduler.portfolio;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.*;

import java.io.IOException;

/**
 * Created by ane on 4/1/16.
 */
public abstract class PluginScheduler<
        A extends SchedulerApplicationAttempt,
        N extends SchedulerNode,
        S extends PluginScheduler<A, N, S>>
        extends AbstractYarnScheduler<A, N> implements Configurable {

    protected Class<A> aClass;
    protected Class<N> nClass;
    protected Configuration pluginConf;

    public PluginScheduler(Class<A> aClass, Class<N> nClass, Class<S> sClass) {
        super(sClass.getName());
        this.aClass = aClass;
        this.nClass = nClass;
    }

    protected void initializePlugin(Configuration conf) {
        this.pluginConf = conf;
    }

    public abstract void handleAddNode();

    @Override
    public void handle(SchedulerEvent event) {
        //TODO define protected methods to handle events
        //TODO replace handle(SchedulerEvent event) call with handle(HandleEventRequest request)
        //TODO keep only exception on this event
//        switch (event.getType()) {
//            case NODE_ADDED:
//                if (!(event instanceof NodeAddedSchedulerEvent)) {
//                    throw new RuntimeException("Unexpected event type: " + event);
//                }
//                NodeAddedSchedulerEvent nodeAddedEvent = (NodeAddedSchedulerEvent)event;
//                addNode(nodeAddedEvent.getAddedRMNode());
//                recoverContainersOnNode(nodeAddedEvent.getContainerReports(),
//                        nodeAddedEvent.getAddedRMNode());
//                break;
//            case NODE_REMOVED:
//                if (!(event instanceof NodeRemovedSchedulerEvent)) {
//                    throw new RuntimeException("Unexpected event type: " + event);
//                }
//                NodeRemovedSchedulerEvent nodeRemovedEvent = (NodeRemovedSchedulerEvent)event;
//                removeNode(nodeRemovedEvent.getRemovedRMNode());
//                break;
//            case NODE_UPDATE:
//                if (!(event instanceof NodeUpdateSchedulerEvent)) {
//                    throw new RuntimeException("Unexpected event type: " + event);
//                }
//                NodeUpdateSchedulerEvent nodeUpdatedEvent = (NodeUpdateSchedulerEvent)event;
//                nodeUpdate(nodeUpdatedEvent.getRMNode());
//                break;
//            case APP_ADDED:
//                if (!(event instanceof AppAddedSchedulerEvent)) {
//                    throw new RuntimeException("Unexpected event type: " + event);
//                }
//                AppAddedSchedulerEvent appAddedEvent = (AppAddedSchedulerEvent) event;
//                String queueName =
//                        resolveReservationQueueName(appAddedEvent.getQueue(),
//                                appAddedEvent.getApplicationId(),
//                                appAddedEvent.getReservationID());
//                if (queueName != null) {
//                    addApplication(appAddedEvent.getApplicationId(),
//                            queueName, appAddedEvent.getUser(),
//                            appAddedEvent.getIsAppRecovering());
//                }
//                break;
//            case APP_REMOVED:
//                if (!(event instanceof AppRemovedSchedulerEvent)) {
//                    throw new RuntimeException("Unexpected event type: " + event);
//                }
//                AppRemovedSchedulerEvent appRemovedEvent = (AppRemovedSchedulerEvent)event;
//                removeApplication(appRemovedEvent.getApplicationID(),
//                        appRemovedEvent.getFinalState());
//                break;
//            case NODE_RESOURCE_UPDATE:
//                if (!(event instanceof NodeResourceUpdateSchedulerEvent)) {
//                    throw new RuntimeException("Unexpected event type: " + event);
//                }
//                NodeResourceUpdateSchedulerEvent nodeResourceUpdatedEvent =
//                        (NodeResourceUpdateSchedulerEvent)event;
//                updateNodeResource(nodeResourceUpdatedEvent.getRMNode(),
//                        nodeResourceUpdatedEvent.getResourceOption());
//                break;
//            case APP_ATTEMPT_ADDED:
//                if (!(event instanceof AppAttemptAddedSchedulerEvent)) {
//                    throw new RuntimeException("Unexpected event type: " + event);
//                }
//                AppAttemptAddedSchedulerEvent appAttemptAddedEvent =
//                        (AppAttemptAddedSchedulerEvent) event;
//                addApplicationAttempt(appAttemptAddedEvent.getApplicationAttemptId(),
//                        appAttemptAddedEvent.getTransferStateFromPreviousAttempt(),
//                        appAttemptAddedEvent.getIsAttemptRecovering());
//                break;
//            case APP_ATTEMPT_REMOVED:
//                if (!(event instanceof AppAttemptRemovedSchedulerEvent)) {
//                    throw new RuntimeException("Unexpected event type: " + event);
//                }
//                AppAttemptRemovedSchedulerEvent appAttemptRemovedEvent =
//                        (AppAttemptRemovedSchedulerEvent) event;
//                removeApplicationAttempt(
//                        appAttemptRemovedEvent.getApplicationAttemptID(),
//                        appAttemptRemovedEvent.getFinalAttemptState(),
//                        appAttemptRemovedEvent.getKeepContainersAcrossAppAttempts());
//                break;
//            case CONTAINER_EXPIRED:
//                if (!(event instanceof ContainerExpiredSchedulerEvent)) {
//                    throw new RuntimeException("Unexpected event type: " + event);
//                }
//                ContainerExpiredSchedulerEvent containerExpiredEvent =
//                        (ContainerExpiredSchedulerEvent)event;
//                ContainerId containerId = containerExpiredEvent.getContainerId();
//                completedContainer(getRMContainer(containerId),
//                        SchedulerUtils.createAbnormalContainerStatus(
//                                containerId,
//                                SchedulerUtils.EXPIRED_CONTAINER),
//                        RMContainerEventType.EXPIRE);
//                break;
//            default:
//                LOG.error("Unknown event arrived at FairScheduler: " + event.toString());
//        }
    }
}
