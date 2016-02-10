package org.apache.hadoop.tools.posum.master.scheduler.data;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.*;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by ane on 1/22/16.
 */
public class DOSAppAttempt extends SchedulerApplicationAttempt {

    private static Log logger = LogFactory.getLog(DataOrientedScheduler.class);

    private final Set<ContainerId> containersToPreempt =
            new HashSet<ContainerId>();


    public DOSAppAttempt(ApplicationAttemptId applicationAttemptId, String user, Queue queue, ActiveUsersManager activeUsersManager, RMContext rmContext) {
        super(applicationAttemptId, user, queue, activeUsersManager, rmContext);
    }

    synchronized public boolean containerCompleted(RMContainer rmContainer,
                                                   ContainerStatus containerStatus, RMContainerEventType event) {

        // Remove from the list of containers
        if (null == liveContainers.remove(rmContainer.getContainerId())) {
            return false;
        }

        // Remove from the list of newly allocated containers if found
        newlyAllocatedContainers.remove(rmContainer);

        Container container = rmContainer.getContainer();
        ContainerId containerId = container.getId();

        // Inform the container
        rmContainer.handle(
                new RMContainerFinishedEvent(
                        containerId,
                        containerStatus,
                        event)
        );
        logger.info("Completed container: " + rmContainer.getContainerId() +
                " in state: " + rmContainer.getState() + " event:" + event);

        containersToPreempt.remove(rmContainer.getContainerId());

        RMAuditLogger.logSuccess(getUser(),
                RMAuditLogger.AuditConstants.RELEASE_CONTAINER, "SchedulerApp",
                getApplicationId(), containerId);

        // Update usage metrics
        Resource containerResource = rmContainer.getContainer().getResource();
        queue.getMetrics().releaseResources(getUser(), 1, containerResource);
        Resources.subtractFrom(currentConsumption, containerResource);

        // Clear resource utilization metrics cache.
        lastMemoryAggregateAllocationUpdateTime = -1;

        return true;
    }

    synchronized public RMContainer allocate(NodeType type, DOSchedulerNode node,
                                             Priority priority, ResourceRequest request,
                                             Container container) {

        if (isStopped) {
            return null;
        }

        // Required sanity check - AM can call 'allocate' to update resource
        // request without locking the scheduler, hence we need to check
        if (getTotalRequiredResources(priority) <= 0) {
            return null;
        }

        // Create RMContainer
        RMContainer rmContainer = new RMContainerImpl(container, this
                .getApplicationAttemptId(), node.getNodeID(),
                appSchedulingInfo.getUser(), this.rmContext);

        // Add it to allContainers list.
        newlyAllocatedContainers.add(rmContainer);
        liveContainers.put(container.getId(), rmContainer);

        // Update consumption and track allocations
        List<ResourceRequest> resourceRequestList = appSchedulingInfo.allocate(
                type, node, priority, request, container);
        Resources.addTo(currentConsumption, container.getResource());

        // Update resource requests related to "request" and store in RMContainer
        ((RMContainerImpl)rmContainer).setResourceRequests(resourceRequestList);

        // Inform the container
        rmContainer.handle(
                new RMContainerEvent(container.getId(), RMContainerEventType.START));

        if (logger.isDebugEnabled()) {
            logger.debug("allocate: applicationAttemptId="
                    + container.getId().getApplicationAttemptId()
                    + " container=" + container.getId() + " host="
                    + container.getNodeId().getHost() + " type=" + type);
        }
        RMAuditLogger.logSuccess(getUser(),
                RMAuditLogger.AuditConstants.ALLOC_CONTAINER, "SchedulerApp",
                getApplicationId(), container.getId());

        return rmContainer;
    }
}
