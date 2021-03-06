package org.apache.hadoop.tools.posum.scheduler.core;

import com.codahale.metrics.SlidingTimeWindowReservoir;
import com.codahale.metrics.Timer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.tools.posum.client.data.DatabaseUtils;
import org.apache.hadoop.tools.posum.client.scheduler.MetaScheduler;
import org.apache.hadoop.tools.posum.common.records.call.StoreLogCall;
import org.apache.hadoop.tools.posum.common.util.PosumException;
import org.apache.hadoop.tools.posum.common.util.conf.PolicyPortfolio;
import org.apache.hadoop.tools.posum.common.util.conf.PosumConfiguration;
import org.apache.hadoop.tools.posum.scheduler.portfolio.PluginPolicy;
import org.apache.hadoop.tools.posum.web.MetaSchedulerWebApp;
import org.apache.hadoop.tools.posum.web.PosumWebApp;
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
import org.apache.hadoop.yarn.api.records.ResourceOption;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.proto.YarnServiceProtos;
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerAppReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplication;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNodeReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.QueueEntitlement;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEventType;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.hadoop.tools.posum.common.records.dataentity.LogEntry.Type.POLICY_CHANGE;

public class PortfolioMetaScheduler extends
  AbstractYarnScheduler<SchedulerApplicationAttempt, SchedulerNode> implements
  Configurable, MetaScheduler {

  private static Log logger = LogFactory.getLog(PortfolioMetaScheduler.class);

  private Configuration conf;
  private Configuration posumConf;
  private MetaSchedulerCommService commService;
  private PolicyPortfolio policies;

  private Class<? extends PluginPolicy> currentPolicyClass;
  private PluginPolicy<? extends SchedulerApplicationAttempt, ? extends SchedulerNode> currentPolicy;
  private ReadWriteLock lock = new ReentrantReadWriteLock();
  private Lock readLock = lock.readLock();
  private Lock writeLock = lock.writeLock();

  private PosumWebApp webApp;

  //metrics
  private Timer allocateTimer;
  private Timer handleTimer;
  private Timer changeTimer;
  private Map<SchedulerEventType, Timer> handleByTypeTimers;
  private boolean metricsON;

  public PortfolioMetaScheduler() {
    super(PortfolioMetaScheduler.class.getName());
  }

  // for testing purposes
  PortfolioMetaScheduler(Configuration posumConf, MetaSchedulerCommService commService) {
    this();
    this.posumConf = posumConf;
    this.commService = commService;
  }

  private PluginPolicy<? extends SchedulerApplicationAttempt, ? extends SchedulerNode> instantiatePolicy(Class<? extends PluginPolicy> currentPolicyClass) {
    PluginPolicy<? extends SchedulerApplicationAttempt, ? extends SchedulerNode> newPolicy;
    try {
      newPolicy = currentPolicyClass.newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
      throw new PosumException("Could not instantiate scheduler for class " + currentPolicyClass, e);
    }
    newPolicy.initializePlugin(posumConf, commService);
    if (rmContext != null)
      newPolicy.setRMContext(rmContext);
    return newPolicy;
  }

  @Override
  public void changeToPolicy(String policyName) {
    logger.debug("Changing policy to " + policyName);
    Class<? extends PluginPolicy> newPolicyClass = policies.get(policyName);
    if (newPolicyClass == null)
      throw new PosumException("Target policy does not exist: " + policyName);
    logPolicyChange(policyName);
    if (!currentPolicyClass.equals(newPolicyClass)) {
      Timer.Context context = null;
      if (metricsON)
        context = changeTimer.time();
      writeLock.lock();
      PluginPolicy oldPolicy = currentPolicy;
      PluginPolicy<? extends SchedulerApplicationAttempt, ? extends SchedulerNode> newPolicy = instantiatePolicy(newPolicyClass);
      if (!isInState(STATE.NOTINITED)) {
        newPolicy.init(conf);
      }
      if (isInState(STATE.STARTED)) {
        newPolicy.transferStateFromPolicy(oldPolicy);
        newPolicy.start();
      }
      currentPolicy = newPolicy;
      currentPolicyClass = newPolicyClass;
      if (oldPolicy.isInState(STATE.STARTED)) {
        oldPolicy.stop();
      }
      writeLock.unlock();
      if (metricsON)
        context.stop();
      logger.debug("Policy changed successfully");
      commService.getDatabase().execute(StoreLogCall.newInstance(DatabaseUtils.newLogEntry("Changed scheduling policy to " + policyName)));
    }
  }

  private void logPolicyChange(String policyName) {
    commService.getDatabase().execute(StoreLogCall.newInstance(DatabaseUtils.newLogEntry(POLICY_CHANGE, policyName)));
  }

  /**
   * Methods that all schedulers seem to override
   */

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public synchronized void setConf(Configuration conf) {
    this.conf = conf;
  }

  public Timer getAllocateTimer() {
    return allocateTimer;
  }

  public Timer getHandleTimer() {
    return handleTimer;
  }

  public Timer getChangeTimer() {
    return changeTimer;
  }

  public Map<SchedulerEventType, Timer> getHandleByTypeTimers() {
    return handleByTypeTimers;
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    if (posumConf == null)
      posumConf = PosumConfiguration.newInstance(conf);
    setConf(posumConf);
    policies = new PolicyPortfolio(posumConf);
    currentPolicyClass = policies.get(policies.getDefaultPolicyName());
    if (commService == null) {
      commService = new MetaSchedulerCommServiceImpl(this, posumConf.get(YarnConfiguration.RM_ADDRESS));
      commService.init(posumConf);
    }
    currentPolicy = instantiatePolicy(currentPolicyClass);
    currentPolicy.init(conf);

    //initialize  metrics
    metricsON = posumConf.getBoolean(PosumConfiguration.SCHEDULER_METRICS_ON, PosumConfiguration.SCHEDULER_METRICS_ON_DEFAULT);

    if (metricsON) {
      long windowSize = posumConf.getLong(PosumConfiguration.POSUM_MONITOR_HEARTBEAT_MS,
        PosumConfiguration.POSUM_MONITOR_HEARTBEAT_MS_DEFAULT);

      allocateTimer = new Timer(new SlidingTimeWindowReservoir(windowSize, TimeUnit.MILLISECONDS));
      handleTimer = new Timer(new SlidingTimeWindowReservoir(windowSize, TimeUnit.MILLISECONDS));
      changeTimer = new Timer(new SlidingTimeWindowReservoir(windowSize, TimeUnit.MILLISECONDS));

      handleByTypeTimers = new HashMap<>();
      for (SchedulerEventType e : SchedulerEventType.values()) {
        Timer timer = new Timer(new SlidingTimeWindowReservoir(windowSize, TimeUnit.MILLISECONDS));
        handleByTypeTimers.put(e, timer);
      }

      //initialize statistics service
      webApp = new MetaSchedulerWebApp(this,
        posumConf.getInt(PosumConfiguration.SCHEDULER_WEBAPP_PORT, PosumConfiguration.SCHEDULER_WEBAPP_PORT_DEFAULT));
    }
  }

  @Override
  public void serviceStart() throws Exception {
    logger.debug("Starting meta");
    commService.start();
    logPolicyChange(policies.getDefaultPolicyName());
    readLock.lock();
    try {
      currentPolicy.start();
    } finally {
      readLock.unlock();
    }
    if (webApp != null)
      webApp.start();
  }

  @Override
  public void serviceStop() throws Exception {
    logger.debug("Stopping meta");
    readLock.lock();
    try {
      if (this.commService != null) {
        this.commService.stop();
      }
      if (this.currentPolicy != null) {
        this.currentPolicy.stop();
      }
    } finally {
      readLock.unlock();
    }
    if (webApp != null)
      webApp.stop();
  }

  @Override
  public int getNumClusterNodes() {
    return currentPolicy.getNumClusterNodes();
  }

  @Override
  public synchronized void setRMContext(RMContext rmContext) {
    this.rmContext = rmContext;
    readLock.lock();
    try {
      if (currentPolicy != null)
        currentPolicy.setRMContext(rmContext);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public synchronized void reinitialize(Configuration conf, RMContext rmContext) throws IOException {
    this.rmContext = rmContext;
    this.conf = conf;
    readLock.lock();
    try {
      if (currentPolicy != null)
        currentPolicy.reinitialize(conf, rmContext);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public Allocation allocate(
    ApplicationAttemptId applicationAttemptId, List<ResourceRequest> ask,
    List<ContainerId> release, List<String> blacklistAdditions, List<String> blacklistRemovals) {
    Timer.Context context = null;
    if (metricsON) {
      context = allocateTimer.time();
    }
    readLock.lock();
    try {
      return currentPolicy.allocate(applicationAttemptId, ask,
        release, blacklistAdditions, blacklistRemovals);
    } finally {
      if (metricsON) {
        context.stop();
      }
      readLock.unlock();
    }
  }

  @Override
  public void handle(SchedulerEvent event) {
    Timer.Context generalContext = null, typeContext = null;
    logger.trace("MetaScheduler received event " + event);
    if (metricsON) {
      generalContext = handleTimer.time();
      typeContext = handleByTypeTimers.get(event.getType()).time();
    }
    readLock.lock();
    try {
      currentPolicy.handle(event);
    } finally {
      if (metricsON) {
        generalContext.stop();
        typeContext.stop();
      }
      readLock.unlock();
    }
  }

  @Override
  public QueueInfo getQueueInfo(String queueName,
                                boolean includeChildQueues, boolean recursive) throws IOException {
    return currentPolicy.getQueueInfo(queueName, includeChildQueues, recursive);
  }

  @Override
  public List<QueueUserACLInfo> getQueueUserAclInfo() {
    return currentPolicy.getQueueUserAclInfo();
  }

  @Override
  public ResourceCalculator getResourceCalculator() {
    return currentPolicy.getResourceCalculator();
  }

  @Override
  public void recover(RMStateStore.RMState state) throws Exception {
    readLock.lock();
    try {
      currentPolicy.recover(state);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public RMContainer getRMContainer(ContainerId containerId) {
    return currentPolicy.getRMContainer(containerId);
  }

  @Override
  protected void completedContainer(RMContainer rmContainer, ContainerStatus containerStatus, RMContainerEventType event) {
    readLock.lock();
    try {
      currentPolicy.forwardCompletedContainer(rmContainer, containerStatus, event);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public QueueMetrics getRootQueueMetrics() {
    return currentPolicy.getRootQueueMetrics();
  }

  @Override
  public synchronized boolean checkAccess(UserGroupInformation callerUGI,
                                          QueueACL acl, String queueName) {
    return currentPolicy.checkAccess(callerUGI, acl, queueName);
  }

  @Override
  public synchronized List<ApplicationAttemptId> getAppsInQueue(String queueName) {
    return currentPolicy.getAppsInQueue(queueName);
  }

  /**
   * Methods that the MetaScheduler must override because it is a dummy scheduler
   */

  @Override
  public synchronized List<Container> getTransferredContainers(ApplicationAttemptId currentAttempt) {
    return currentPolicy.getTransferredContainers(currentAttempt);
  }

  @Override
  public Map<ApplicationId, SchedulerApplication<SchedulerApplicationAttempt>> getSchedulerApplications() {
    // explicit conversion required due to currentPolicy outputting SchedulerApplication<? extends SchedulerApplicationAttempt>>
    Map<ApplicationId, ? extends SchedulerApplication<? extends SchedulerApplicationAttempt>> apps =
      currentPolicy.getSchedulerApplications();
    Map<ApplicationId, SchedulerApplication<SchedulerApplicationAttempt>> ret = new HashMap<>(apps.size());

    for (Map.Entry<ApplicationId, ? extends SchedulerApplication<? extends SchedulerApplicationAttempt>> entry :
      apps.entrySet()) {
      ret.put(entry.getKey(), (SchedulerApplication<SchedulerApplicationAttempt>) entry.getValue());
    }
    return ret;
  }

  @Override
  public Resource getClusterResource() {
    return currentPolicy.getClusterResource();
  }

  @Override
  public Resource getMinimumResourceCapability() {
    return currentPolicy.getMinimumResourceCapability();
  }

  @Override
  public Resource getMaximumResourceCapability() {
    return currentPolicy.getMaximumResourceCapability();
  }

  @Override
  public Resource getMaximumResourceCapability(String queueName) {
    return currentPolicy.getMaximumResourceCapability(queueName);
  }

  @Override
  public SchedulerApplicationAttempt getApplicationAttempt(ApplicationAttemptId applicationAttemptId) {
    return currentPolicy.getApplicationAttempt(applicationAttemptId);
  }

  @Override
  public SchedulerAppReport getSchedulerAppInfo(ApplicationAttemptId appAttemptId) {
    return currentPolicy.getSchedulerAppInfo(appAttemptId);
  }

  @Override
  public ApplicationResourceUsageReport getAppResourceUsageReport(ApplicationAttemptId appAttemptId) {
    return currentPolicy.getAppResourceUsageReport(appAttemptId);
  }

  @Override
  public SchedulerApplicationAttempt getCurrentAttemptForContainer(ContainerId containerId) {
    return currentPolicy.getCurrentAttemptForContainer(containerId);
  }

  @Override
  public SchedulerNodeReport getNodeReport(NodeId nodeId) {
    return currentPolicy.getNodeReport(nodeId);
  }

  @Override
  public String moveApplication(ApplicationId appId, String newQueue) throws YarnException {
    readLock.lock();
    try {
      return currentPolicy.moveApplication(appId, newQueue);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public void removeQueue(String queueName) throws YarnException {
    readLock.lock();
    try {
      currentPolicy.removeQueue(queueName);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public void addQueue(Queue newQueue) throws YarnException {
    readLock.lock();
    try {
      currentPolicy.addQueue(newQueue);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public void setEntitlement(String queue, QueueEntitlement entitlement) throws YarnException {
    readLock.lock();
    try {
      currentPolicy.setEntitlement(queue, entitlement);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public synchronized void recoverContainersOnNode(List<NMContainerStatus> containerReports, RMNode nm) {
    readLock.lock();
    try {
      currentPolicy.recoverContainersOnNode(containerReports, nm);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public SchedulerNode getSchedulerNode(NodeId nodeId) {
    readLock.lock();
    try {
      return currentPolicy.getSchedulerNode(nodeId);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public synchronized void moveAllApps(String sourceQueue, String destQueue) throws YarnException {
    logger.debug("Acquiring read lock34");
    readLock.lock();
    try {
      currentPolicy.moveAllApps(sourceQueue, destQueue);
    } finally {
      readLock.unlock();
      logger.debug("Unlocked read lock34");
    }
  }

  @Override
  public synchronized void killAllAppsInQueue(String queueName) throws YarnException {
    logger.debug("Acquiring read lock45");
    readLock.lock();
    try {
      currentPolicy.killAllAppsInQueue(queueName);
    } finally {
      readLock.unlock();
      logger.debug("Unlocked read lock45");
    }
  }

  @Override
  public synchronized void updateNodeResource(RMNode nm, ResourceOption resourceOption) {
    logger.debug("Acquiring read lock56");
    readLock.lock();
    try {
      currentPolicy.updateNodeResource(nm, resourceOption);
    } finally {
      readLock.unlock();
      logger.debug("Unlocked read lock56");
    }
  }

  @Override
  public EnumSet<YarnServiceProtos.SchedulerResourceTypes> getSchedulingResourceTypes() {
    return currentPolicy.getSchedulingResourceTypes();
  }

  @Override
  public Set<String> getPlanQueues() throws YarnException {
    return currentPolicy.getPlanQueues();
  }

  @Override
  public List<ResourceRequest> getPendingResourceRequestsForAttempt(ApplicationAttemptId attemptId) {
    return currentPolicy.getPendingResourceRequestsForAttempt(attemptId);
  }

  @Override
  protected void initMaximumResourceCapability(Resource maximumAllocation) {
    readLock.lock();
    try {
      currentPolicy.forwardInitMaximumResourceCapability(maximumAllocation);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  protected synchronized void containerLaunchedOnNode(ContainerId containerId, SchedulerNode node) {
    readLock.lock();
    try {
      currentPolicy.forwardContainerLaunchedOnNode(containerId, node);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  protected void recoverResourceRequestForContainer(RMContainer rmContainer) {
    readLock.lock();
    try {
      currentPolicy.forwardRecoverResourceRequestForContainer(rmContainer);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  protected void createReleaseCache() {
    readLock.lock();
    try {
      currentPolicy.forwardCreateReleaseCache();
    } finally {
      readLock.unlock();
    }
  }

  @Override
  protected void releaseContainers(List<ContainerId> containers, SchedulerApplicationAttempt attempt) {
    readLock.lock();
    try {
      currentPolicy.forwardReleaseContainers(containers, attempt);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  protected void updateMaximumAllocation(SchedulerNode node, boolean add) {
    readLock.lock();
    try {
      currentPolicy.forwardUpdateMaximumAllocation(node, add);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  protected void refreshMaximumAllocation(Resource newMaxAlloc) {
    readLock.lock();
    try {
      currentPolicy.forwardRefreshMaximumAllocation(newMaxAlloc);
    } finally {
      readLock.unlock();
    }
  }

  public boolean hasMetricsOn() {
    return metricsON;
  }

  public Map<String, SchedulerNodeReport> getNodeReports() {
    return currentPolicy.getNodeReports();
  }
}
