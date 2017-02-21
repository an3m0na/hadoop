package org.apache.hadoop.tools.posum.simulation.core.resourcemanager;

import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.tools.posum.simulation.core.SimulationConfiguration;
import org.apache.hadoop.tools.posum.simulation.core.SimulationContext;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerAppReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplication;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNodeReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEventType;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Private
@Unstable
public class ResourceSchedulerWrapper
  extends AbstractYarnScheduler<SchedulerApplicationAttempt, SchedulerNode>
  implements ResourceScheduler, Configurable {

  private final Logger LOG = Logger.getLogger(ResourceSchedulerWrapper.class);

  private Configuration conf;
  private ResourceScheduler scheduler;
  private SimulationContext simulationContext;

  public ResourceSchedulerWrapper() {
    super(ResourceSchedulerWrapper.class.getName());
  }

  public void setSimulationContext(SimulationContext simulationContext){
    this.simulationContext = simulationContext;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    // set scheduler
    Class<? extends ResourceScheduler> klass =
      conf.getClass(SimulationConfiguration.RM_SCHEDULER, null,
        ResourceScheduler.class);

    scheduler = ReflectionUtils.newInstance(klass, conf);
  }

  @Override
  public Allocation allocate(ApplicationAttemptId attemptId,
                             List<ResourceRequest> resourceRequests,
                             List<ContainerId> containerIds,
                             List<String> strings, List<String> strings2) {

    return scheduler.allocate(attemptId,
      resourceRequests, containerIds, strings, strings2);
  }

  @Override
  public void handle(SchedulerEvent schedulerEvent) {
    scheduler.handle(schedulerEvent);

    if (schedulerEvent.getType() == SchedulerEventType.APP_REMOVED
      && schedulerEvent instanceof AppRemovedSchedulerEvent) {
      simulationContext.getRemainingJobsCounter().countDown();
    }
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void serviceInit(Configuration conf) throws Exception {
    ((AbstractYarnScheduler<SchedulerApplicationAttempt, SchedulerNode>)
      scheduler).init(conf);
    super.serviceInit(conf);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void serviceStart() throws Exception {
    ((AbstractYarnScheduler<SchedulerApplicationAttempt, SchedulerNode>)
      scheduler).start();
    super.serviceStart();
  }

  @SuppressWarnings("unchecked")
  @Override
  public void serviceStop() throws Exception {
    ((AbstractYarnScheduler<SchedulerApplicationAttempt, SchedulerNode>)
      scheduler).stop();
    super.serviceStop();
  }

  @Override
  public void setRMContext(RMContext rmContext) {
    scheduler.setRMContext(rmContext);
  }

  @Override
  public void reinitialize(Configuration conf, RMContext rmContext)
    throws IOException {
    scheduler.reinitialize(conf, rmContext);
  }

  @Override
  public void recover(RMStateStore.RMState rmState) throws Exception {
    scheduler.recover(rmState);
  }

  @Override
  public QueueInfo getQueueInfo(String s, boolean b, boolean b2)
    throws IOException {
    return scheduler.getQueueInfo(s, b, b2);
  }

  @Override
  public List<QueueUserACLInfo> getQueueUserAclInfo() {
    return scheduler.getQueueUserAclInfo();
  }

  @Override
  public Resource getMinimumResourceCapability() {
    return scheduler.getMinimumResourceCapability();
  }

  @Override
  public Resource getMaximumResourceCapability() {
    return scheduler.getMaximumResourceCapability();
  }

  @Override
  public ResourceCalculator getResourceCalculator() {
    return scheduler.getResourceCalculator();
  }

  @Override
  public int getNumClusterNodes() {
    return scheduler.getNumClusterNodes();
  }

  @Override
  public SchedulerNodeReport getNodeReport(NodeId nodeId) {
    return scheduler.getNodeReport(nodeId);
  }

  @Override
  public SchedulerAppReport getSchedulerAppInfo(
    ApplicationAttemptId attemptId) {
    return scheduler.getSchedulerAppInfo(attemptId);
  }

  @Override
  public QueueMetrics getRootQueueMetrics() {
    return scheduler.getRootQueueMetrics();
  }

  @Override
  public synchronized boolean checkAccess(UserGroupInformation callerUGI,
                                          QueueACL acl, String queueName) {
    return scheduler.checkAccess(callerUGI, acl, queueName);
  }

  @Override
  public ApplicationResourceUsageReport getAppResourceUsageReport(
    ApplicationAttemptId appAttemptId) {
    return scheduler.getAppResourceUsageReport(appAttemptId);
  }

  @Override
  public List<ApplicationAttemptId> getAppsInQueue(String queue) {
    return scheduler.getAppsInQueue(queue);
  }

  @Override
  public RMContainer getRMContainer(ContainerId containerId) {
    return null;
  }

  @Override
  public String moveApplication(ApplicationId appId, String newQueue)
    throws YarnException {
    return scheduler.moveApplication(appId, newQueue);
  }

  @Override
  @LimitedPrivate("yarn")
  @Unstable
  public Resource getClusterResource() {
    return null;
  }

  @Override
  public synchronized List<Container> getTransferredContainers(
    ApplicationAttemptId currentAttempt) {
    return new ArrayList<>();
  }

  @Override
  public Map<ApplicationId, SchedulerApplication<SchedulerApplicationAttempt>>
  getSchedulerApplications() {
    return new HashMap<>();
  }

  @Override
  protected void completedContainer(RMContainer rmContainer,
                                    ContainerStatus containerStatus, RMContainerEventType event) {
    // do nothing
  }
}

