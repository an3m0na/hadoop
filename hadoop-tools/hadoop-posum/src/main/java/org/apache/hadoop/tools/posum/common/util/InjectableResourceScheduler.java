package org.apache.hadoop.tools.posum.common.util;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.tools.posum.scheduler.portfolio.PluginPolicy;
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
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class InjectableResourceScheduler<T extends AbstractYarnScheduler> extends AbstractYarnScheduler<SchedulerApplicationAttempt, SchedulerNode>
  implements ResourceScheduler, Configurable {

  private Class<T> schedulerClass;
  private Configuration conf;
  private T scheduler;
  private DatabaseProvider databaseProvider;

  public InjectableResourceScheduler(Class<T> schedulerClass,
                                     DatabaseProvider databaseProvider) {
    super(InjectableResourceScheduler.class.getName());
    this.schedulerClass = schedulerClass;
    this.databaseProvider = databaseProvider;
  }

  public InjectableResourceScheduler(T scheduler) {
    super(InjectableResourceScheduler.class.getName());
    this.scheduler = scheduler;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    if (scheduler == null) {
      this.scheduler = ReflectionUtils.newInstance(schedulerClass, conf);
      if (scheduler instanceof PluginPolicy) {
        ((PluginPolicy) scheduler).initializePlugin(conf, databaseProvider);
      }
    }
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
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void serviceInit(Configuration conf) throws Exception {
    scheduler.init(conf);
    super.serviceInit(conf);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void serviceStart() throws Exception {
    scheduler.start();
    super.serviceStart();
  }

  @SuppressWarnings("unchecked")
  @Override
  public void serviceStop() throws Exception {
    scheduler.stop();
    super.serviceStop();
  }

  @Override
  public void setRMContext(RMContext rmContext) {
    scheduler.setRMContext(rmContext);
  }

  @Override
  public void reinitialize(Configuration conf, RMContext rmContext) throws IOException {
    scheduler.reinitialize(conf, rmContext);
  }

  @Override
  public void recover(RMStateStore.RMState rmState) throws Exception {
    scheduler.recover(rmState);
  }

  @Override
  public QueueInfo getQueueInfo(String s, boolean b, boolean b2) throws IOException {
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
  public SchedulerAppReport getSchedulerAppInfo(ApplicationAttemptId attemptId) {
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
  public ApplicationResourceUsageReport getAppResourceUsageReport(ApplicationAttemptId appAttemptId) {
    return scheduler.getAppResourceUsageReport(appAttemptId);
  }

  @Override
  public List<ApplicationAttemptId> getAppsInQueue(String queue) {
    return scheduler.getAppsInQueue(queue);
  }

  @Override
  public RMContainer getRMContainer(ContainerId containerId) {
    return scheduler.getRMContainer(containerId);
  }

  @Override
  public String moveApplication(ApplicationId appId, String newQueue) throws YarnException {
    return scheduler.moveApplication(appId, newQueue);
  }

  public Resource getClusterResource() {
    return scheduler.getClusterResource();
  }

  @SuppressWarnings("unchecked")
  @Override
  public synchronized List<Container> getTransferredContainers(ApplicationAttemptId currentAttempt) {
    return scheduler.getTransferredContainers(currentAttempt);
  }

  @SuppressWarnings("unchecked")
  @Override
  public Map<ApplicationId, SchedulerApplication<SchedulerApplicationAttempt>> getSchedulerApplications() {
    return scheduler.getSchedulerApplications();
  }

  @Override
  protected void completedContainer(RMContainer rmContainer,
                                    ContainerStatus containerStatus, RMContainerEventType event) {
    if (scheduler instanceof PluginPolicy)
      ((PluginPolicy) scheduler).forwardCompletedContainer(rmContainer, containerStatus, event);
  }

  @Override
  public SchedulerApplicationAttempt getApplicationAttempt(ApplicationAttemptId applicationAttemptId) {
    return scheduler.getApplicationAttempt(applicationAttemptId);
  }
}
