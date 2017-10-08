package org.apache.hadoop.tools.posum.scheduler.portfolio.common;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.tools.posum.common.util.PosumException;
import org.apache.hadoop.tools.posum.common.util.Utils;
import org.apache.hadoop.tools.posum.scheduler.portfolio.PluginApplicationAttempt;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AppSchedulingInfo;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityHeadroomProvider;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;

import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class FiCaPluginApplicationAttempt extends FiCaSchedulerApp implements PluginApplicationAttempt {
  private static Log LOG = LogFactory.getLog(FiCaPluginApplicationAttempt.class);
  private FiCaSchedulerApp core;

  public FiCaPluginApplicationAttempt(ApplicationAttemptId applicationAttemptId, String user, Queue queue, ActiveUsersManager activeUsersManager, RMContext rmContext) {
    super(applicationAttemptId, user, queue, activeUsersManager, rmContext);
    this.core = null;
  }

  public <T extends SchedulerApplicationAttempt & PluginApplicationAttempt> FiCaPluginApplicationAttempt(T predecessor,
                                                                                                         ActiveUsersManager activeUsersManager,
                                                                                                         RMContext rmContext) {
    super(predecessor.getApplicationAttemptId(), predecessor.getUser(), predecessor.getQueue(), activeUsersManager, rmContext); // not used
    SchedulerApplicationAttempt coreCandidate = predecessor.getCoreApp();
    if (coreCandidate != null && coreCandidate instanceof FiCaSchedulerApp)
      this.core = ((FiCaSchedulerApp) coreCandidate);
    else if (predecessor instanceof FiCaSchedulerApp)
      this.core = (FiCaSchedulerApp) predecessor;
    else {
      LOG.error("Cannot safely create instance of " + getClass() + " from type " + predecessor.getClass() + ". Runtime issues may arise");
      this.core = null;
    }
  }

  @Override
  public SchedulerApplicationAttempt getCoreApp() {
    return core;
  }

  public static <A extends FiCaPluginApplicationAttempt> A getInstance(Class<A> aClass,
                                                                       ApplicationAttemptId applicationAttemptId,
                                                                       String user,
                                                                       Queue queue,
                                                                       ActiveUsersManager activeUsersManager,
                                                                       RMContext rmContext) {
    try {
      Constructor<A> constructor = aClass.getConstructor(ApplicationAttemptId.class, String.class, Queue.class,
        ActiveUsersManager.class, RMContext.class);
      return constructor.newInstance(applicationAttemptId, user, queue, activeUsersManager, rmContext);
    } catch (Exception e) {
      throw new PosumException("Failed to instantiate app attempt via default constructor" + e);
    }
  }

  public static <A extends FiCaPluginApplicationAttempt> A getInstance(Class<A> aClass,
                                                                       SchedulerApplicationAttempt attempt,
                                                                       ActiveUsersManager activeUsersManager,
                                                                       RMContext rmContext) {
    try {
      Constructor<A> constructor = aClass.getConstructor(SchedulerApplicationAttempt.class, ActiveUsersManager.class, RMContext.class);
      return constructor.newInstance(attempt, activeUsersManager, rmContext);
    } catch (Exception e) {
      throw new PosumException("Failed to instantiate app attempt via default constructor" + e);
    }
  }

  @Override
  public boolean containerCompleted(RMContainer rmContainer,
                                    ContainerStatus containerStatus, RMContainerEventType event) {
    if (core != null) {
      return core.containerCompleted(rmContainer, containerStatus, event);
    }
    return super.containerCompleted(rmContainer, containerStatus, event);
  }

  @Override
  public synchronized RMContainer allocate(NodeType type, FiCaSchedulerNode node, Priority priority, ResourceRequest request, Container container) {
    if (core != null) {
      return core.allocate(type, node, priority, request, container);
    }
    return super.allocate(type, node, priority, request, container);
  }

  @Override
  public synchronized boolean unreserve(FiCaSchedulerNode node, Priority priority) {
    if (core != null) {
      return core.unreserve(node, priority);
    }
    return super.unreserve(node, priority);
  }

  @Override
  public synchronized float getLocalityWaitFactor(Priority priority, int clusterNodes) {
    if (core != null) {
      return core.getLocalityWaitFactor(priority, clusterNodes);
    }
    return super.getLocalityWaitFactor(priority, clusterNodes);
  }

  @Override
  public synchronized Resource getTotalPendingRequests() {
    if (core != null) {
      return core.getTotalPendingRequests();
    }
    return super.getTotalPendingRequests();
  }

  @Override
  public synchronized void addPreemptContainer(ContainerId cont) {
    if (core != null) {
      Utils.invokeMethod(core, FiCaSchedulerApp.class, "addPreemptContainer", new Class<?>[]{ContainerId.class}, cont);
      return;
    }
    super.addPreemptContainer(cont);
  }

  @Override
  public synchronized Allocation getAllocation(ResourceCalculator rc, Resource clusterResource, Resource minimumAllocation) {
    if (core != null) {
      return core.getAllocation(rc, clusterResource, minimumAllocation);
    }
    return super.getAllocation(rc, clusterResource, minimumAllocation);
  }

  @Override
  public synchronized NodeId getNodeIdToUnreserve(Priority priority, Resource resourceNeedUnreserve, ResourceCalculator rc, Resource clusterResource) {
    if (core != null) {
      return core.getNodeIdToUnreserve(priority, resourceNeedUnreserve, rc, clusterResource);
    }
    return super.getNodeIdToUnreserve(priority, resourceNeedUnreserve, rc, clusterResource);
  }

  @Override
  public synchronized void setHeadroomProvider(CapacityHeadroomProvider headroomProvider) {
    if (core != null) {
      Utils.writeField(core, FiCaSchedulerApp.class, "headroomProvider", headroomProvider);
      return;
    }
    super.setHeadroomProvider(headroomProvider);
  }

  @Override
  public synchronized CapacityHeadroomProvider getHeadroomProvider() {
    if (core != null) {
      return super.getHeadroomProvider();
    }
    return super.getHeadroomProvider();
  }

  @Override
  protected synchronized void resetReReservations(Priority priority) {
    if (core != null) {
      Utils.invokeMethod(core, SchedulerApplicationAttempt.class, "resetReReservations", new Class<?>[]{Priority.class}, priority);
      return;
    }
    super.resetReReservations(priority);
  }

  @Override
  protected synchronized void addReReservation(Priority priority) {
    if (core != null) {
      Utils.invokeMethod(core, SchedulerApplicationAttempt.class, "addReReservation", new Class<?>[]{Priority.class}, priority);
      return;
    }
    super.addReReservation(priority);
  }

  @Override
  public synchronized Collection<RMContainer> getLiveContainers() {
    if (core != null) {
      return core.getLiveContainers();
    }
    return super.getLiveContainers();
  }

  @Override
  public AppSchedulingInfo getAppSchedulingInfo() {
    if (core != null) {
      return core.getAppSchedulingInfo();
    }
    return super.getAppSchedulingInfo();
  }

  @Override
  public boolean isPending() {
    if (core != null) {
      return core.isPending();
    }
    return super.isPending();
  }

  @Override
  public ApplicationAttemptId getApplicationAttemptId() {
    if (core != null) {
      return core.getApplicationAttemptId();
    }
    return super.getApplicationAttemptId();
  }

  @Override
  public ApplicationId getApplicationId() {
    if (core != null) {
      return core.getApplicationId();
    }
    return super.getApplicationId();
  }

  @Override
  public String getUser() {
    if (core != null) {
      return core.getUser();
    }
    return super.getUser();
  }

  @Override
  public Map<String, ResourceRequest> getResourceRequests(Priority priority) {
    if (core != null) {
      return core.getResourceRequests(priority);
    }
    return super.getResourceRequests(priority);
  }

  @Override
  public Set<ContainerId> getPendingRelease() {
    if (core != null) {
      return core.getPendingRelease();
    }
    return super.getPendingRelease();
  }

  @Override
  public long getNewContainerId() {
    if (core != null) {
      return core.getNewContainerId();
    }
    return super.getNewContainerId();
  }

  @Override
  public Collection<Priority> getPriorities() {

    if (core != null) {
      return core.getPriorities();
    }
    return super.getPriorities();
  }

  @Override
  public synchronized ResourceRequest getResourceRequest(Priority priority, String resourceName) {
    if (core != null) {
      return core.getResourceRequest(priority, resourceName);
    }
    return super.getResourceRequest(priority, resourceName);
  }

  @Override
  public synchronized int getTotalRequiredResources(Priority priority) {
    if (core != null) {
      return core.getTotalRequiredResources(priority);
    }
    return super.getTotalRequiredResources(priority);
  }

  @Override
  public synchronized Resource getResource(Priority priority) {
    if (core != null) {
      return core.getResource(priority);
    }
    return super.getResource(priority);
  }

  @Override
  public String getQueueName() {
    if (core != null) {
      return core.getQueueName();
    }
    return super.getQueueName();
  }

  @Override
  public Resource getAMResource() {
    if (core != null) {
      return core.getAMResource();
    }
    return super.getAMResource();
  }

  @Override
  public void setAMResource(Resource amResource) {
    if (core != null) {
      core.setAMResource(amResource);
      return;
    }
    super.setAMResource(amResource);
  }

  @Override
  public boolean isAmRunning() {
    if (core != null) {
      return core.isAmRunning();
    }
    return super.isAmRunning();
  }

  @Override
  public void setAmRunning(boolean bool) {
    if (core != null) {
      core.setAmRunning(bool);
      return;
    }
    super.setAmRunning(bool);
  }

  @Override
  public boolean getUnmanagedAM() {
    if (core != null) {
      return core.getUnmanagedAM();
    }
    return super.getUnmanagedAM();
  }

  @Override
  public synchronized RMContainer getRMContainer(ContainerId id) {
    if (core != null) {
      return core.getRMContainer(id);
    }
    return super.getRMContainer(id);
  }

  @Override
  public synchronized int getReReservations(Priority priority) {
    if (core != null) {
      return core.getReReservations(priority);
    }
    return super.getReReservations(priority);
  }

  @Override
  public synchronized Resource getCurrentReservation() {
    if (core != null) {
      return core.getCurrentReservation();
    }
    return super.getCurrentReservation();
  }

  @Override
  public Queue getQueue() {
    if (core != null) {
      return core.getQueue();
    }
    return super.getQueue();
  }

  @Override
  public synchronized void updateResourceRequests(List<ResourceRequest> requests) {
    if (core != null) {
      core.updateResourceRequests(requests);
      return;
    }
    super.updateResourceRequests(requests);
  }

  @Override
  public synchronized void recoverResourceRequests(List<ResourceRequest> requests) {
    if (core != null) {
      core.recoverResourceRequests(requests);
      return;
    }
    super.recoverResourceRequests(requests);
  }

  @Override
  public synchronized void stop(RMAppAttemptState rmAppAttemptFinalState) {
    if (core != null) {
      core.stop(rmAppAttemptFinalState);
      return;
    }
    super.stop(rmAppAttemptFinalState);
  }

  @Override
  public synchronized boolean isStopped() {
    if (core != null) {
      return core.isStopped();
    }
    return super.isStopped();
  }

  @Override
  public synchronized List<RMContainer> getReservedContainers() {
    if (core != null) {
      return core.getReservedContainers();
    }
    return super.getReservedContainers();
  }

  @Override
  public synchronized RMContainer reserve(SchedulerNode node, Priority priority, RMContainer rmContainer, Container container) {
    if (core != null) {
      return core.reserve(node, priority, rmContainer, container);
    }
    return super.reserve(node, priority, rmContainer, container);
  }

  @Override
  public synchronized boolean isReserved(SchedulerNode node, Priority priority) {
    if (core != null) {
      return core.isReserved(node, priority);
    }
    return super.isReserved(node, priority);
  }

  @Override
  public synchronized void setHeadroom(Resource globalLimit) {
    if (core != null) {
      core.setHeadroom(globalLimit);
      return;
    }
    super.setHeadroom(globalLimit);
  }

  @Override
  public synchronized Resource getHeadroom() {
    if (core != null) {
      return core.getHeadroom();
    }
    return super.getHeadroom();
  }

  @Override
  public synchronized int getNumReservedContainers(Priority priority) {
    if (core != null) {
      return core.getNumReservedContainers(priority);
    }
    return super.getNumReservedContainers(priority);
  }

  @Override
  public synchronized void containerLaunchedOnNode(ContainerId containerId, NodeId nodeId) {
    if (core != null) {
      core.containerLaunchedOnNode(containerId, nodeId);
      return;
    }
    super.containerLaunchedOnNode(containerId, nodeId);
  }

  @Override
  public synchronized void showRequests() {
    if (core != null) {
      core.showRequests();
      return;
    }
    super.showRequests();
  }

  @Override
  public Resource getCurrentConsumption() {
    if (core != null) {
      return core.getCurrentConsumption();
    }
    return super.getCurrentConsumption();
  }

  @Override
  public synchronized ContainersAndNMTokensAllocation pullNewlyAllocatedContainersAndNMTokens() {
    if (core != null) {
      return core.pullNewlyAllocatedContainersAndNMTokens();
    }
    return super.pullNewlyAllocatedContainersAndNMTokens();
  }

  @Override
  public synchronized void updateBlacklist(List<String> blacklistAdditions, List<String> blacklistRemovals) {
    if (core != null) {
      core.updateBlacklist(blacklistAdditions, blacklistRemovals);
      return;
    }
    super.updateBlacklist(blacklistAdditions, blacklistRemovals);
  }

  @Override
  public boolean isBlacklisted(String resourceName) {
    if (core != null) {
      return core.isBlacklisted(resourceName);
    }
    return super.isBlacklisted(resourceName);
  }

  @Override
  public synchronized void addSchedulingOpportunity(Priority priority) {
    if (core != null) {
      core.addSchedulingOpportunity(priority);
      return;
    }
    super.addSchedulingOpportunity(priority);
  }

  @Override
  public synchronized void subtractSchedulingOpportunity(Priority priority) {
    if (core != null) {
      core.subtractSchedulingOpportunity(priority);
      return;
    }
    super.subtractSchedulingOpportunity(priority);
  }

  @Override
  public synchronized int getSchedulingOpportunities(Priority priority) {
    if (core != null) {
      return core.getSchedulingOpportunities(priority);
    }
    return super.getSchedulingOpportunities(priority);
  }

  @Override
  public synchronized void resetSchedulingOpportunities(Priority priority) {
    if (core != null) {
      core.resetSchedulingOpportunities(priority);
      return;
    }
    super.resetSchedulingOpportunities(priority);
  }

  @Override
  public synchronized void resetSchedulingOpportunities(Priority priority, long currentTimeMs) {
    if (core != null) {
      core.resetSchedulingOpportunities(priority, currentTimeMs);
      return;
    }
    super.resetSchedulingOpportunities(priority, currentTimeMs);
  }

  @Override
  public synchronized ApplicationResourceUsageReport getResourceUsageReport() {
    if (core != null) {
      return core.getResourceUsageReport();
    }
    return super.getResourceUsageReport();
  }

  @Override
  public synchronized Map<ContainerId, RMContainer> getLiveContainersMap() {
    if (core != null) {
      return core.getLiveContainersMap();
    }
    return super.getLiveContainersMap();
  }

  @Override
  public synchronized Resource getResourceLimit() {
    if (core != null) {
      return core.getResourceLimit();
    }
    return super.getResourceLimit();
  }

  @Override
  public synchronized Map<Priority, Long> getLastScheduledContainer() {
    if (core != null) {
      return core.getLastScheduledContainer();
    }
    return super.getLastScheduledContainer();
  }

  @Override
  public synchronized void transferStateFromPreviousAttempt(SchedulerApplicationAttempt appAttempt) {
    if (core != null) {
      core.transferStateFromPreviousAttempt(appAttempt);
      return;
    }
    super.transferStateFromPreviousAttempt(appAttempt);
  }

  @Override
  public synchronized void move(Queue newQueue) {
    if (core != null) {
      core.move(newQueue);
      return;
    }
    super.move(newQueue);
  }

  @Override
  public synchronized void recoverContainer(RMContainer rmContainer) {
    if (core != null) {
      core.recoverContainer(rmContainer);
      return;
    }
    super.recoverContainer(rmContainer);
  }

  @Override
  public void incNumAllocatedContainers(NodeType containerType, NodeType requestType) {
    if (core != null) {
      core.incNumAllocatedContainers(containerType, requestType);
      return;
    }
    super.incNumAllocatedContainers(containerType, requestType);
  }

  @Override
  public void setApplicationHeadroomForMetrics(Resource headroom) {
    if (core != null) {
      core.setApplicationHeadroomForMetrics(headroom);
      return;
    }
    super.setApplicationHeadroomForMetrics(headroom);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "=" + getApplicationId() + "\n" +
      "      " + "Consumption: " + getCurrentConsumption() + "\n" +
      "      " + "Containers: " + getLiveContainersMap() + "\n" +
      "      " + "ResourceRequests: " + getAppSchedulingInfo().getAllResourceRequests();
  }
}
