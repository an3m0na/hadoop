package org.apache.hadoop.tools.posum.core.scheduler.portfolio.singleq;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.tools.posum.common.util.PosumException;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.*;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.*;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.lang.reflect.Constructor;
import java.util.*;

/**
 * Created by ane on 1/22/16.
 */
public class SQSAppAttempt extends SchedulerApplicationAttempt {

    private static Log logger = LogFactory.getLog(SQSAppAttempt.class);

    private final SQSAppAttempt inner;
    private final boolean viaInner;

    private final Set<ContainerId> containersToPreempt =
            new HashSet<ContainerId>();


    public SQSAppAttempt(ApplicationAttemptId applicationAttemptId, String user, Queue queue, ActiveUsersManager activeUsersManager, RMContext rmContext) {
        super(applicationAttemptId, user, queue, activeUsersManager, rmContext);
        this.inner = this;
        this.viaInner = false;
    }

    public SQSAppAttempt(SQSAppAttempt inner) {
        super(inner.getApplicationAttemptId(), inner.getUser(), inner.getQueue(), null, inner.rmContext);
        this.inner = inner;
        this.viaInner = true;
    }

    static <A extends SQSAppAttempt> A getInstance(Class<A> aClass, ApplicationAttemptId applicationAttemptId, String user, Queue queue, ActiveUsersManager activeUsersManager, RMContext rmContext) {
        try {
            Constructor<A> constructor = aClass.getConstructor(ApplicationAttemptId.class, String.class, Queue.class, ActiveUsersManager.class, RMContext.class);
            return constructor.newInstance(applicationAttemptId, user, queue, activeUsersManager, rmContext);
        } catch (Exception e) {
            throw new PosumException("Failed to instantiate app attempt via default constructor" + e);
        }
    }

    static <A extends SQSAppAttempt> A getInstance(Class<A> aClass, SQSAppAttempt attempt) {
        try {
            Constructor<A> constructor = aClass.getConstructor(SQSAppAttempt.class);
            return constructor.newInstance(attempt);
        } catch (Exception e) {
            throw new PosumException("Failed to instantiate app attempt via default constructor" + e);
        }
    }

    synchronized public boolean containerCompleted(RMContainer rmContainer,
                                                   ContainerStatus containerStatus, RMContainerEventType event) {

        if (viaInner) {
            return inner.containerCompleted(rmContainer, containerStatus, event);
        }

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

    synchronized public RMContainer allocate(NodeType type, SQSchedulerNode node,
                                             Priority priority, ResourceRequest request,
                                             Container container) {

        if (viaInner) {
            return inner.allocate(type, node, priority, request, container);
        }

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
        ((RMContainerImpl) rmContainer).setResourceRequests(resourceRequestList);

        // Inform the container
        rmContainer.handle(
                new RMContainerEvent(container.getId(), RMContainerEventType.START));

        if (logger.isDebugEnabled()) {
            logger.trace("allocate: applicationAttemptId="
                    + container.getId().getApplicationAttemptId()
                    + " container=" + container.getId() + " host="
                    + container.getNodeId().getHost() + " type=" + type);
        }
        RMAuditLogger.logSuccess(getUser(),
                RMAuditLogger.AuditConstants.ALLOC_CONTAINER, "SchedulerApp",
                getApplicationId(), container.getId());

        return rmContainer;
    }

    @Override
    public synchronized Collection<RMContainer> getLiveContainers() {
        if (viaInner) {
            return inner.getLiveContainers();
        }
        return super.getLiveContainers();
    }

    @Override
    public AppSchedulingInfo getAppSchedulingInfo() {
        if (viaInner) {
            return inner.getAppSchedulingInfo();
        }
        return super.getAppSchedulingInfo();
    }

    @Override
    public boolean isPending() {
        if (viaInner) {
            return inner.isPending();
        }
        return super.isPending();
    }

    @Override
    public ApplicationAttemptId getApplicationAttemptId() {
        if (viaInner) {
            return inner.getApplicationAttemptId();
        }
        return super.getApplicationAttemptId();
    }

    @Override
    public ApplicationId getApplicationId() {
        if (viaInner) {
            return inner.getApplicationId();
        }
        return super.getApplicationId();
    }

    @Override
    public String getUser() {
        if (viaInner) {
            return inner.getUser();
        }
        return super.getUser();
    }

    @Override
    public Map<String, ResourceRequest> getResourceRequests(Priority priority) {
        if (viaInner) {
            return inner.getResourceRequests(priority);
        }
        return super.getResourceRequests(priority);
    }

    @Override
    public Set<ContainerId> getPendingRelease() {
        if (viaInner) {
            return inner.getPendingRelease();
        }
        return super.getPendingRelease();
    }

    @Override
    public long getNewContainerId() {
        if (viaInner) {
            return inner.getNewContainerId();
        }
        return super.getNewContainerId();
    }

    @Override
    public Collection<Priority> getPriorities() {

        if (viaInner) {
            return inner.getPriorities();
        }
        return super.getPriorities();
    }

    @Override
    public synchronized ResourceRequest getResourceRequest(Priority priority, String resourceName) {
        if (viaInner) {
            return inner.getResourceRequest(priority, resourceName);
        }
        return super.getResourceRequest(priority, resourceName);
    }

    @Override
    public synchronized int getTotalRequiredResources(Priority priority) {
        if (viaInner) {
            return inner.getTotalRequiredResources(priority);
        }
        return super.getTotalRequiredResources(priority);
    }

    @Override
    public synchronized Resource getResource(Priority priority) {
        if (viaInner) {
            return inner.getResource(priority);
        }
        return super.getResource(priority);
    }

    @Override
    public String getQueueName() {
        if (viaInner) {
            return inner.getQueueName();
        }
        return super.getQueueName();
    }

    @Override
    public Resource getAMResource() {
        if (viaInner) {
            return inner.getAMResource();
        }
        return super.getAMResource();
    }

    @Override
    public void setAMResource(Resource amResource) {
        if (viaInner) {
            inner.setAMResource(amResource);
            return;
        }
        super.setAMResource(amResource);
    }

    @Override
    public boolean isAmRunning() {
        if (viaInner) {
            return inner.isAmRunning();
        }
        return super.isAmRunning();
    }

    @Override
    public void setAmRunning(boolean bool) {
        if (viaInner) {
            inner.setAmRunning(bool);
            return;
        }
        super.setAmRunning(bool);
    }

    @Override
    public boolean getUnmanagedAM() {
        if (viaInner) {
            return inner.getUnmanagedAM();
        }
        return super.getUnmanagedAM();
    }

    @Override
    public synchronized RMContainer getRMContainer(ContainerId id) {
        if (viaInner) {
            return inner.getRMContainer(id);
        }
        return super.getRMContainer(id);
    }

    @Override
    protected synchronized void resetReReservations(Priority priority) {
        if (viaInner) {
            inner.resetReReservations(priority);
            return;
        }
        super.resetReReservations(priority);
    }

    @Override
    protected synchronized void addReReservation(Priority priority) {
        if (viaInner) {
            inner.addReReservation(priority);
            return;
        }
        super.addReReservation(priority);
    }

    @Override
    public synchronized int getReReservations(Priority priority) {
        if (viaInner) {
            return inner.getReReservations(priority);
        }
        return super.getReReservations(priority);
    }

    @Override
    public synchronized Resource getCurrentReservation() {
        if (viaInner) {
            return inner.getCurrentReservation();
        }
        return super.getCurrentReservation();
    }

    @Override
    public Queue getQueue() {
        if (viaInner) {
            return inner.getQueue();
        }
        return super.getQueue();
    }

    @Override
    public synchronized void updateResourceRequests(List<ResourceRequest> requests) {
        if (viaInner) {
            inner.updateResourceRequests(requests);
            return;
        }
        super.updateResourceRequests(requests);
    }

    @Override
    public synchronized void recoverResourceRequests(List<ResourceRequest> requests) {
        if (viaInner) {
            inner.recoverResourceRequests(requests);
            return;
        }
        super.recoverResourceRequests(requests);
    }

    @Override
    public synchronized void stop(RMAppAttemptState rmAppAttemptFinalState) {
        if (viaInner) {
            inner.stop(rmAppAttemptFinalState);
            return;
        }
        super.stop(rmAppAttemptFinalState);
    }

    @Override
    public synchronized boolean isStopped() {
        if (viaInner) {
            return inner.isStopped();
        }
        return super.isStopped();
    }

    @Override
    public synchronized List<RMContainer> getReservedContainers() {
        if (viaInner) {
            return inner.getReservedContainers();
        }
        return super.getReservedContainers();
    }

    @Override
    public synchronized RMContainer reserve(SchedulerNode node, Priority priority, RMContainer rmContainer, Container container) {
        if (viaInner) {
            return inner.reserve(node, priority, rmContainer, container);
        }
        return super.reserve(node, priority, rmContainer, container);
    }

    @Override
    public synchronized boolean isReserved(SchedulerNode node, Priority priority) {
        if (viaInner) {
            return inner.isReserved(node, priority);
        }
        return super.isReserved(node, priority);
    }

    @Override
    public synchronized void setHeadroom(Resource globalLimit) {
        if (viaInner) {
            inner.setHeadroom(globalLimit);
            return;
        }
        super.setHeadroom(globalLimit);
    }

    @Override
    public synchronized Resource getHeadroom() {
        if (viaInner) {
            return inner.getHeadroom();
        }
        return super.getHeadroom();
    }

    @Override
    public synchronized int getNumReservedContainers(Priority priority) {
        if (viaInner) {
            return inner.getNumReservedContainers(priority);
        }
        return super.getNumReservedContainers(priority);
    }

    @Override
    public synchronized void containerLaunchedOnNode(ContainerId containerId, NodeId nodeId) {
        if (viaInner) {
            inner.containerLaunchedOnNode(containerId, nodeId);
            return;
        }
        super.containerLaunchedOnNode(containerId, nodeId);
    }

    @Override
    public synchronized void showRequests() {
        if (viaInner) {
            inner.showRequests();
            return;
        }
        super.showRequests();
    }

    @Override
    public Resource getCurrentConsumption() {
        if (viaInner) {
            return inner.getCurrentConsumption();
        }
        return super.getCurrentConsumption();
    }

    @Override
    public synchronized ContainersAndNMTokensAllocation pullNewlyAllocatedContainersAndNMTokens() {
        if (viaInner) {
            return inner.pullNewlyAllocatedContainersAndNMTokens();
        }
        return super.pullNewlyAllocatedContainersAndNMTokens();
    }

    @Override
    public synchronized void updateBlacklist(List<String> blacklistAdditions, List<String> blacklistRemovals) {
        if (viaInner) {
            inner.updateBlacklist(blacklistAdditions, blacklistRemovals);
            return;
        }
        super.updateBlacklist(blacklistAdditions, blacklistRemovals);
    }

    @Override
    public boolean isBlacklisted(String resourceName) {
        if (viaInner) {
            return inner.isBlacklisted(resourceName);
        }
        return super.isBlacklisted(resourceName);
    }

    @Override
    public synchronized void addSchedulingOpportunity(Priority priority) {
        if (viaInner) {
            inner.addSchedulingOpportunity(priority);
            return;
        }
        super.addSchedulingOpportunity(priority);
    }

    @Override
    public synchronized void subtractSchedulingOpportunity(Priority priority) {
        if (viaInner) {
            inner.subtractSchedulingOpportunity(priority);
            return;
        }
        super.subtractSchedulingOpportunity(priority);
    }

    @Override
    public synchronized int getSchedulingOpportunities(Priority priority) {
        if (viaInner) {
            return inner.getSchedulingOpportunities(priority);
        }
        return super.getSchedulingOpportunities(priority);
    }

    @Override
    public synchronized void resetSchedulingOpportunities(Priority priority) {
        if (viaInner) {
            inner.resetSchedulingOpportunities(priority);
            return;
        }
        super.resetSchedulingOpportunities(priority);
    }

    @Override
    public synchronized void resetSchedulingOpportunities(Priority priority, long currentTimeMs) {
        if (viaInner) {
            inner.resetSchedulingOpportunities(priority, currentTimeMs);
            return;
        }
        super.resetSchedulingOpportunities(priority, currentTimeMs);
    }

    @Override
    public synchronized ApplicationResourceUsageReport getResourceUsageReport() {
        if (viaInner) {
            return inner.getResourceUsageReport();
        }
        return super.getResourceUsageReport();
    }

    @Override
    public synchronized Map<ContainerId, RMContainer> getLiveContainersMap() {
        if (viaInner) {
            return inner.getLiveContainersMap();
        }
        return super.getLiveContainersMap();
    }

    @Override
    public synchronized Resource getResourceLimit() {
        if (viaInner) {
            return inner.getResourceLimit();
        }
        return super.getResourceLimit();
    }

    @Override
    public synchronized Map<Priority, Long> getLastScheduledContainer() {
        if (viaInner) {
            return inner.getLastScheduledContainer();
        }
        return super.getLastScheduledContainer();
    }

    @Override
    public synchronized void transferStateFromPreviousAttempt(SchedulerApplicationAttempt appAttempt) {
        if (viaInner) {
            inner.transferStateFromPreviousAttempt(appAttempt);
            return;
        }
        super.transferStateFromPreviousAttempt(appAttempt);
    }

    @Override
    public synchronized void move(Queue newQueue) {
        if (viaInner) {
            inner.move(newQueue);
            return;
        }
        super.move(newQueue);
    }

    @Override
    public synchronized void recoverContainer(RMContainer rmContainer) {
        if (viaInner) {
            inner.recoverContainer(rmContainer);
            return;
        }
        super.recoverContainer(rmContainer);
    }

    @Override
    public void incNumAllocatedContainers(NodeType containerType, NodeType requestType) {
        if (viaInner) {
            inner.incNumAllocatedContainers(containerType, requestType);
            return;
        }
        super.incNumAllocatedContainers(containerType, requestType);
    }

    @Override
    public void setApplicationHeadroomForMetrics(Resource headroom) {
        if (viaInner) {
            inner.setApplicationHeadroomForMetrics(headroom);
            return;
        }
        super.setApplicationHeadroomForMetrics(headroom);
    }
}
