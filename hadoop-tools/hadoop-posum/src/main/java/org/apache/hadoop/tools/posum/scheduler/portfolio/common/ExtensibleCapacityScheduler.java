package org.apache.hadoop.tools.posum.scheduler.portfolio.common;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang.mutable.MutableObject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.tools.posum.common.util.PosumException;
import org.apache.hadoop.tools.posum.common.util.GeneralUtils;
import org.apache.hadoop.tools.posum.common.util.cluster.ClusterUtils;
import org.apache.hadoop.tools.posum.common.util.communication.DatabaseProvider;
import org.apache.hadoop.tools.posum.common.util.conf.PosumConfiguration;
import org.apache.hadoop.tools.posum.scheduler.portfolio.PluginPolicy;
import org.apache.hadoop.tools.posum.scheduler.portfolio.PluginPolicyState;
import org.apache.hadoop.tools.posum.scheduler.portfolio.PluginSchedulerNode;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceOption;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.proto.YarnServiceProtos;
import org.apache.hadoop.yarn.security.YarnAuthorizationProvider;
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.PreemptableResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceLimits;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerAppReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplication;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerDynamicEditException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNodeReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AbstractCSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.LeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.ParentQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.QueueEntitlement;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.utils.Lock;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_QUEUE_NAME;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.MAXIMUM_APPLICATION_MASTERS_RESOURCE_PERCENT;

public abstract class ExtensibleCapacityScheduler<
  A extends FiCaPluginApplicationAttempt,
  N extends FiCaPluginSchedulerNode>
  extends PluginPolicy<A, N> implements PreemptableResourceScheduler, CapacitySchedulerContext {

  private static Log LOG = LogFactory.getLog(ExtensibleCapacityScheduler.class);

  private CapacitySchedulerConfiguration capacityConf;
  protected final CapacityScheduler inner;

  public ExtensibleCapacityScheduler(Class<A> aClass,
                                     Class<N> nClass,
                                     String policyName) {
    super(aClass, nClass, policyName);
    this.inner = new CapacityScheduler();
  }

  @Override
  public void initializePlugin(Configuration conf, DatabaseProvider dbProvider) {
    super.initializePlugin(conf, dbProvider);
    float maxAMRatio = conf.getFloat(PosumConfiguration.MAX_AM_RATIO, PosumConfiguration.MAX_AM_RATIO_DEFAULT);
    getConf().setFloat(MAXIMUM_APPLICATION_MASTERS_RESOURCE_PERCENT, maxAMRatio);
  }

  //
  // <------ Helper reflection methods for accessing internal CapacityScheduler methods and fields ------>
  //

  protected void writeField(String name, Object value) {
    GeneralUtils.writeField(inner, CapacityScheduler.class, name, value);
  }

  protected <T> T readField(String name) {
    return GeneralUtils.readField(inner, CapacityScheduler.class, name);
  }


  protected <T> T invokeMethod(String name, Class<?>[] paramTypes, Object... args) {
    return GeneralUtils.invokeMethod(inner, CapacityScheduler.class, name, paramTypes, args);
  }

  //
  // <------ / Helper reflection methods for accessing internal CapacityScheduler methods and fields ------>
  //

  //
  // <------ Methods to be overridden when extending scheduler ------>
  //

  /**
   * Override this to load a custom configuration for the underlying capacity scheduler.
   * By default, the configuration remains the same as the one in capacity-scheduler.xml
   *
   * @param conf the initial YARN configuration
   * @return the custom CapacitySchedulerConfiguration
   */
  protected CapacitySchedulerConfiguration loadCustomCapacityConf(Configuration conf) {
    return readField("conf");
  }

  /**
   * Override this to populate custom fields of an instance of A to be used for prioritisation within a queue.
   * By default, the priority is its ApplicationId, so no addSource is necessary
   *
   * @param app the application attempt
   */
  protected void updateAppPriority(A app) {

  }

  /**
   * Override this to add custom logic when application attempt is being added.
   * It is called after SchedulerApplication::setCurrentAppAttempt(A) and before notifying the queue.
   * By default, only the updateAppPriority(A) method is called
   *
   * @param app the application attempt
   */
  protected void onAppAttemptAdded(A app) {
    updateAppPriority(app);
  }

  /**
   * Override this to modify the target queue of an application when it is added to the scheduler
   *
   * @param queue           intended queue name from the app added event
   * @param applicationId   the application id
   * @param isAppRecovering from the app added event; usually not necessary
   * @param reservationID   from the app added event; usually not necessary
   * @return the revised target queue name
   */
  protected String resolveQueue(String queue,
                                ApplicationId applicationId,
                                String user,
                                boolean isAppRecovering,
                                ReservationId reservationID) {
    return queue;
  }

  /**
   * Acts the same as resolveQueue, but is used when the scheduler has just been initialized and must accept the
   * applications from a previous scheduler
   *
   * @param applicationId the id of the application from the previous scheduler
   * @param application   the application from the previous scheduler
   * @return the target queue name for this scheduler
   */
  protected String resolveMoveQueue(ApplicationId applicationId, SchedulerApplication<? extends SchedulerApplicationAttempt> application) {
    //TODO
    return DEFAULT_QUEUE_NAME;
  }

  /**
   * Override to add custom logic to whether priorities should be updated.
   * This is called whenever NODE_UPDATE is received, before trying to allocate.
   * By default, priorities never expire (ApplicationId is constant after app is initially added)
   *
   * @return true = priorities have expired and need recalculation
   */
  protected boolean checkIfPrioritiesExpired() {
    return false;
  }

  /**
   * Override this to apply priority updates to queues recursively.
   * By default, the queues are parsed RLF and in each leaf queue
   * updateApplicationPriorities(LeafQueue, String) is applied
   *
   * @param queue current queue (starting with root)
   */
  protected void updateApplicationPriorities(CSQueue queue) {
    if (queue instanceof LeafQueue) {
      updateApplicationPriorities((LeafQueue) queue);
    } else {
      for (CSQueue csQueue : queue.getChildQueues()) {
        updateApplicationPriorities(csQueue);
      }
    }
  }

  /**
   * Override this to apply priority updates to a leaf queue's application sets.
   * By default, each instance of A is passed to updateAppPriority(A) in order
   *
   * @param queue target leaf queue
   */
  protected void updateApplicationPriorities(LeafQueue queue) {
    Set<A> oldActiveApps = GeneralUtils.readField(queue, LeafQueue.class, "activeApplications");
    Set<A> oldApps = GeneralUtils.readField(queue, LeafQueue.class, "pendingApplications");
    boolean appsPending = !oldApps.isEmpty();
    oldApps.addAll(oldActiveApps);
    if (oldApps.isEmpty())
      return;
    Set<A> activeApps = new TreeSet<>(getApplicationComparator());
    Set<A> extraApps = new TreeSet<>(getApplicationComparator());
    for (A app : oldApps) {
      updateAppPriority(app);
      if (!appsPending || !app.isPending()) {
        activeApps.add(app);
        if (!oldActiveApps.contains(app))
          activateApp(queue, app);
      } else {
        extraApps.add(app);
        if (oldActiveApps.contains(app))
          deactivateApp(queue, app);
      }
    }
    GeneralUtils.writeField(queue, LeafQueue.class, "activeApplications", activeApps);
    // add the rest of the apps to pending
    GeneralUtils.writeField(queue, LeafQueue.class, "pendingApplications", extraApps);
    if (appsPending) // not all apps were active, so retry activation
      GeneralUtils.invokeMethod(queue, LeafQueue.class, "activateApplications", new Class[]{});
  }

  private void activateApp(LeafQueue queue, A app) {
    LeafQueue.User user = queue.getUser(app.getUser());
    user.activateApplication();
    user.getResourceUsage().incAMUsed(app.getAMResource());
    queue.getQueueResourceUsage().incAMUsed(app.getAMResource());
  }

  private void deactivateApp(LeafQueue queue, A app) {
    LeafQueue.User user = queue.getUser(app.getUser());
    user.finishApplication(true);
    user.getResourceUsage().decAMUsed(app.getAMResource());
    queue.getQueueResourceUsage().decAMUsed(app.getAMResource());
  }

  /**
   * Override in order to change the priorities of applications within a single leaf queue.
   * The default comparator orders by ApplicationId lexicographically
   *
   * @return an instance of a comparator that orders elements of type A
   */
  @Override
  public Comparator<FiCaSchedulerApp> getApplicationComparator() {
    return inner.getApplicationComparator();
  }

  /**
   * Override in order to change the priorities of queues.
   * The default comparator orders according to queue.getUsedCapacity()
   *
   * @return an instance of a comparator that orders queues
   */
  @Override
  public Comparator<CSQueue> getQueueComparator() {
    return inner.getQueueComparator();
  }


  //
  // <------ / Methods to be overridden when extending scheduler ------>
  //

  //
  // <------ Adapted methods ------>
  //

  protected void addApplicationAttempt(
    ApplicationAttemptId applicationAttemptId,
    boolean transferStateFromPreviousAttempt,
    boolean isAttemptRecovering) {

    synchronized (inner) {
      SchedulerApplication<FiCaSchedulerApp> application =
        inner.getSchedulerApplications().get(applicationAttemptId.getApplicationId());
      CSQueue queue = (CSQueue) application.getQueue();

      A attempt = FiCaPluginApplicationAttempt.getInstance(aClass, applicationAttemptId, application.getUser(),
        queue, queue.getActiveUsersManager(), inner.getRMContext());
      if (attempt instanceof Configurable)
        ((Configurable) attempt).setConf(getConf());

      if (transferStateFromPreviousAttempt) {
        attempt.transferStateFromPreviousAttempt(application
          .getCurrentAppAttempt());
      }
      application.setCurrentAppAttempt(attempt);

      onAppAttemptAdded(attempt);

      LOG.debug("Submitting app attempt to queue: \n" + attempt);

      queue.submitApplicationAttempt(attempt, application.getUser());
      LOG.info("Added Application Attempt " + applicationAttemptId
        + " to scheduler from user " + application.getUser() + " in queue "
        + queue.getQueueName());
      if (isAttemptRecovering) {
        if (LOG.isDebugEnabled()) {
          LOG.debug(applicationAttemptId
            + " is recovering. Skipping notifying ATTEMPT_ADDED");
        }
      } else {
        inner.getRMContext().getDispatcher().getEventHandler().handle(
          new RMAppAttemptEvent(applicationAttemptId,
            RMAppAttemptEventType.ATTEMPT_ADDED));
      }
    }
  }

  protected void addNode(RMNode nodeManager) {
    synchronized (inner) {
      N schedulerNode = FiCaPluginSchedulerNode.getInstance(nClass, nodeManager,
        capacityConf.getUsePortForNodeName(), nodeManager.getNodeLabels());
      Map<NodeId, FiCaSchedulerNode> nodes = readField("nodes");
      nodes.put(nodeManager.getNodeID(), schedulerNode);
      Resources.addTo(getClusterResource(), nodeManager.getTotalCapability());

      // addSource this node to node label manager
      RMNodeLabelsManager labelManager = readField("labelManager");
      if (labelManager != null) {
        labelManager.activateNode(nodeManager.getNodeID(),
          nodeManager.getTotalCapability());
      }

      CSQueue root = readField("root");
      root.updateClusterResource(getClusterResource(), new ResourceLimits(
        getClusterResource()));
      int numNodes = this.<AtomicInteger>readField("numNodeManagers").incrementAndGet();
      updateMaximumAllocation(schedulerNode, true);

      LOG.info("Added node " + nodeManager.getNodeAddress() +
        " clusterResource: " + getClusterResource());

      //FIXME uncomment if scheduleAsynchronously becomes available
//            if (scheduleAsynchronously && numNodes == 1) {
//                asyncSchedulerThread.beginSchedule();
//            }
    }
  }

  protected void allocateContainersToNode(FiCaSchedulerNode node) {
    if (checkIfPrioritiesExpired()) {
      synchronized (inner) {
        updateApplicationPriorities(this.<CSQueue>readField("root"));
      }
    }
    if (LOG.isTraceEnabled())
      LOG.trace(printQueues());
    invokeMethod("allocateContainersToNode", new Class<?>[]{FiCaSchedulerNode.class}, node);
  }

  protected void initializeWithModifiedConf() {
    synchronized (inner) {
      writeField("conf", capacityConf);
      invokeMethod("validateConf", new Class<?>[]{Configuration.class}, capacityConf);
      writeField("calculator", capacityConf.getResourceCalculator());
      writeField("queueComparator", getQueueComparator());
      writeField("applicationComparator", getApplicationComparator());
      String[] queues = capacityConf.getQueues(CapacitySchedulerConfiguration.ROOT);
      String initializationMethod = queues.length == 1 && queues[0].equals(DEFAULT_QUEUE_NAME) ? "reinitializeQueues" : "initializeQueues";
      invokeMethod(initializationMethod, new Class<?>[]{CapacitySchedulerConfiguration.class}, capacityConf);
      //asynchronous scheduling is disabled by default, in order to have control over the scheduling cycle
      writeField("scheduleAsynchronously", false);
      LOG.info("Overwrote CapacityScheduler with: " +
        "calculator=" + getResourceCalculator().getClass() + ", " +
        "asynchronousScheduling=false" + ", " +
        "queues=" + Arrays.asList(capacityConf.getQueues(CapacitySchedulerConfiguration.ROOT)));
    }
  }

  protected void reinitializeWithModifiedConf() {
    synchronized (inner) {
      writeField("conf", capacityConf);
      invokeMethod("validateConf", new Class<?>[]{Configuration.class}, capacityConf);
      invokeMethod("reinitializeQueues", new Class<?>[]{CapacitySchedulerConfiguration.class}, capacityConf);
    }
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    // this call will use default capacity configurations in capacity-scheduler.xml
    setConf(conf);
    inner.init(conf);
    // reinitialize with the given capacity scheduler configuration
    capacityConf = loadCustomCapacityConf(conf);
    initializeWithModifiedConf();
  }

  @Override
  public void reinitialize(Configuration conf, RMContext rmContext) throws IOException {
    // this will reinitialize with default capacity scheduler configuration found in capacity-scheduler.xml
    inner.reinitialize(conf, rmContext);
    // reinitialize with the given capacity scheduler configuration
    capacityConf = loadCustomCapacityConf(conf);
    writeField("conf", capacityConf);
    reinitializeWithModifiedConf();
  }

  @Override
  public void handle(SchedulerEvent event) {
    super.handle(event);
    switch (event.getType()) {
      case NODE_ADDED: {
        NodeAddedSchedulerEvent nodeAddedEvent = (NodeAddedSchedulerEvent) event;
        addNode(nodeAddedEvent.getAddedRMNode());
        recoverContainersOnNode(nodeAddedEvent.getContainerReports(),
          nodeAddedEvent.getAddedRMNode());
      }
      break;
      case NODE_UPDATE: {
        NodeUpdateSchedulerEvent nodeUpdatedEvent = (NodeUpdateSchedulerEvent) event;
        RMNode node = nodeUpdatedEvent.getRMNode();
        invokeMethod("nodeUpdate", new Class<?>[]{RMNode.class}, node);
        //FIXME uncomment if scheduleAsynchronously becomes available
//                if (!scheduleAsynchronously) {
        allocateContainersToNode(getNode(node.getNodeID()));
//                }
      }
      break;
      case APP_ADDED: {
        AppAddedSchedulerEvent appAddedEvent = (AppAddedSchedulerEvent) event;
        inner.handle(new AppAddedSchedulerEvent(appAddedEvent.getApplicationId(),
          resolveQueue(appAddedEvent.getQueue(),
            appAddedEvent.getApplicationId(),
            appAddedEvent.getUser(),
            appAddedEvent.getIsAppRecovering(),
            appAddedEvent.getReservationID()),
          appAddedEvent.getUser(),
          appAddedEvent.getIsAppRecovering(),
          appAddedEvent.getReservationID()));
      }
      break;
      case APP_ATTEMPT_ADDED: {
        AppAttemptAddedSchedulerEvent appAttemptAddedEvent =
          (AppAttemptAddedSchedulerEvent) event;
        addApplicationAttempt(appAttemptAddedEvent.getApplicationAttemptId(),
          appAttemptAddedEvent.getTransferStateFromPreviousAttempt(),
          appAttemptAddedEvent.getIsAttemptRecovering());
      }
      break;
      default:
        inner.handle(event);
    }
  }

  @Override
  public Map<ApplicationId, SchedulerApplication<A>> getSchedulerApplications() {
    return readField("applications");
  }

  //
  // <------ / Adapted methods ------>
  //

  //
  // <------ Delegation methods ------>
  //

  @Override
  public void serviceStart() throws Exception {
    inner.serviceStart();
  }

  @Override
  public void serviceStop() throws Exception {
    inner.serviceStop();
  }

  @VisibleForTesting
  public static void setQueueAcls(YarnAuthorizationProvider authorizer, Map<String, CSQueue> queues) throws IOException {
    CapacityScheduler.setQueueAcls(authorizer, queues);
  }

  public CSQueue getQueue(String queueName) {
    return inner.getQueue(queueName);
  }

  @Override
  @Lock(Lock.NoLock.class)
  public Allocation allocate(ApplicationAttemptId applicationAttemptId, List<ResourceRequest> ask, List<ContainerId> release, List<String> blacklistAdditions, List<String> blacklistRemovals) {
    return inner.allocate(applicationAttemptId, ask, release, blacklistAdditions, blacklistRemovals);
  }

  @Override
  @Lock(Lock.NoLock.class)
  public QueueInfo getQueueInfo(String queueName, boolean includeChildQueues, boolean recursive) throws IOException {
    return inner.getQueueInfo(queueName, includeChildQueues, recursive);
  }

  @Override
  @Lock(Lock.NoLock.class)
  public List<QueueUserACLInfo> getQueueUserAclInfo() {
    return inner.getQueueUserAclInfo();
  }

  @Override
  protected void completedContainer(RMContainer rmContainer, ContainerStatus containerStatus, RMContainerEventType event) {
    invokeMethod("completedContainer", new Class<?>[]{RMContainer.class, ContainerStatus.class, RMContainerEventType.class},
      rmContainer,
      containerStatus,
      event);
  }

  @Lock(Lock.NoLock.class)
  @VisibleForTesting
  @Override
  public A getApplicationAttempt(ApplicationAttemptId applicationAttemptId) {
    return (A) inner.getApplicationAttempt(applicationAttemptId);
  }

  @Override
  @Lock(Lock.NoLock.class)
  public FiCaSchedulerNode getNode(NodeId nodeId) {
    return inner.getNode(nodeId);
  }

  @Override
  @Lock(Lock.NoLock.class)
  public void recover(RMStateStore.RMState state) throws Exception {
    inner.recover(state);
  }

  @Override
  public void dropContainerReservation(RMContainer container) {
    inner.dropContainerReservation(container);
  }

  @Override
  public void preemptContainer(ApplicationAttemptId aid, RMContainer cont) {
    inner.preemptContainer(aid, cont);
  }

  @Override
  public void killContainer(RMContainer cont) {
    inner.killContainer(cont);
  }

  @Override
  public boolean checkAccess(UserGroupInformation callerUGI, QueueACL acl, String queueName) {
    return inner.checkAccess(callerUGI, acl, queueName);
  }

  @Override
  public List<ApplicationAttemptId> getAppsInQueue(String queueName) {
    return inner.getAppsInQueue(queueName);
  }

  @Override
  public void removeQueue(String queueName) throws SchedulerDynamicEditException {
    inner.removeQueue(queueName);
  }

  @Override
  public void addQueue(Queue queue) throws SchedulerDynamicEditException {
    inner.addQueue(queue);
  }

  @Override
  public void setEntitlement(String inQueue, QueueEntitlement entitlement) throws SchedulerDynamicEditException, YarnException {
    inner.setEntitlement(inQueue, entitlement);
  }

  @Override
  public String moveApplication(ApplicationId appId, String targetQueueName) throws YarnException {
    return inner.moveApplication(appId, targetQueueName);
  }

  @Override
  public EnumSet<YarnServiceProtos.SchedulerResourceTypes> getSchedulingResourceTypes() {
    return inner.getSchedulingResourceTypes();
  }

  @Override
  public Resource getMaximumResourceCapability(String queueName) {
    return inner.getMaximumResourceCapability(queueName);
  }

  @Override
  public Set<String> getPlanQueues() {
    return inner.getPlanQueues();
  }

  @Override
  public List<Container> getTransferredContainers(ApplicationAttemptId currentAttempt) {
    return inner.getTransferredContainers(currentAttempt);
  }

  @Override
  public Resource getClusterResource() {
    return inner.getClusterResource();
  }

  @Override
  public Resource getMinimumResourceCapability() {
    return inner.getMinimumResourceCapability();
  }

  @Override
  public Resource getMaximumResourceCapability() {
    return inner.getMaximumResourceCapability();
  }

  @Override
  public void initMaximumResourceCapability(Resource maximumAllocation) {
    invokeMethod("initMaximumResourceCapability", new Class<?>[]{Resource.class}, maximumAllocation);
  }

  @Override
  public void containerLaunchedOnNode(ContainerId containerId, SchedulerNode node) {
    invokeMethod("containerLaunchedOnNode", new Class<?>[]{ContainerId.class, SchedulerNode.class}, containerId, node);
  }

  @Override
  public SchedulerAppReport getSchedulerAppInfo(ApplicationAttemptId appAttemptId) {
    return inner.getSchedulerAppInfo(appAttemptId);
  }

  @Override
  public ApplicationResourceUsageReport getAppResourceUsageReport(ApplicationAttemptId appAttemptId) {
    return inner.getAppResourceUsageReport(appAttemptId);
  }

  @Override
  public A getCurrentAttemptForContainer(ContainerId containerId) {
    return (A) inner.getCurrentAttemptForContainer(containerId);
  }

  @Override
  public RMContainer getRMContainer(ContainerId containerId) {
    return inner.getRMContainer(containerId);
  }

  @Override
  public SchedulerNodeReport getNodeReport(NodeId nodeId) {
    return inner.getNodeReport(nodeId);
  }

  @Override
  public void recoverContainersOnNode(List<NMContainerStatus> containerReports, RMNode nm) {
    inner.recoverContainersOnNode(containerReports, nm);
  }

  @Override
  public void recoverResourceRequestForContainer(RMContainer rmContainer) {
    invokeMethod("recoverResourceRequestForContainer", new Class<?>[]{RMContainer.class}, rmContainer);
  }

  @Override
  public void createReleaseCache() {
    invokeMethod("createReleaseCache", new Class<?>[]{});
  }

  @Override
  public void releaseContainers(List<ContainerId> containers, SchedulerApplicationAttempt attempt) {
    invokeMethod("releaseContainers", new Class<?>[]{List.class, SchedulerApplicationAttempt.class}, containers, attempt);
  }

  @Override
  public SchedulerNode getSchedulerNode(NodeId nodeId) {
    return inner.getSchedulerNode(nodeId);
  }

  @Override
  public void moveAllApps(String sourceQueue, String destQueue) throws YarnException {
    inner.moveAllApps(sourceQueue, destQueue);
  }

  @Override
  public void killAllAppsInQueue(String queueName) throws YarnException {
    inner.killAllAppsInQueue(queueName);
  }

  @Override
  public void updateNodeResource(RMNode nm, ResourceOption resourceOption) {
    inner.updateNodeResource(nm, resourceOption);
  }

  @Override
  public void updateMaximumAllocation(SchedulerNode node, boolean add) {
    invokeMethod("updateMaximumAllocation", new Class<?>[]{SchedulerNode.class, boolean.class}, node, add);
  }

  @Override
  public void refreshMaximumAllocation(Resource newMaxAlloc) {
    invokeMethod("refreshMaximumAllocation", new Class<?>[]{Resource.class}, newMaxAlloc);
  }

  @Override
  public List<ResourceRequest> getPendingResourceRequestsForAttempt(ApplicationAttemptId attemptId) {
    return inner.getPendingResourceRequestsForAttempt(attemptId);
  }

  @Override
  public void setConf(Configuration conf) {
    inner.setConf(conf);
  }

  @Override
  public Configuration getConf() {
    return inner.getConf();
  }

  @VisibleForTesting
  public String getMappedQueueForTest(String user) throws IOException {
    return inner.getMappedQueueForTest(user);
  }

  @Override
  public QueueMetrics getRootQueueMetrics() {
    return inner.getRootQueueMetrics();
  }

  public CSQueue getRootQueue() {
    return inner.getRootQueue();
  }

  @Override
  public CapacitySchedulerConfiguration getConfiguration() {
    return inner.getConfiguration();
  }

  @Override
  public RMContainerTokenSecretManager getContainerTokenSecretManager() {
    return inner.getContainerTokenSecretManager();
  }

  @Override
  public ResourceCalculator getResourceCalculator() {
    return inner.getResourceCalculator();
  }

  @Override
  public int getNumClusterNodes() {
    return inner.getNumClusterNodes();
  }

  @Override
  public RMContext getRMContext() {
    return inner.getRMContext();
  }

  @Override
  public void setRMContext(RMContext rmContext) {
    inner.setRMContext(rmContext);
  }

  //
  // <------ Added state accessors ------>
  //

  protected RMNodeLabelsManager getLabelManager() {
    return readField("labelManager");
  }

  protected Map<NodeId, N> getNodes() {
    return readField("nodes");
  }

  public String printQueues() {
    try {
      StringBuilder builder = new StringBuilder("Queues are:\n");
      return printQueues(builder, this.<CSQueue>readField("root")).toString();
    } catch (Exception e) {
      LOG.trace(e);
      return "Could not print queues due to error";
    }
  }

  public StringBuilder printQueues(StringBuilder builder, CSQueue queue) {
    builder.append(queue.toString()).append("\n");
    if (queue instanceof ParentQueue) {
      ParentQueue parent = (ParentQueue) queue;
      for (CSQueue child : parent.getChildQueues()) {
        builder = printQueues(builder, child);
      }
    } else {
      LeafQueue leaf = (LeafQueue) queue;
      Set<A> pendingApplications =
        GeneralUtils.readField(leaf, LeafQueue.class, "pendingApplications");
      builder.append(" --pending:\n");
      for (A app : pendingApplications) {
        builder.append("   ").append(app);
      }
      Set<A> activeApplications =
        GeneralUtils.readField(leaf, LeafQueue.class, "activeApplications");
      builder.append(" --active:\n");
      for (A app : activeApplications) {
        builder.append("   ").append(app);
      }
    }
    return builder;
  }

  //
  // <------ / Added state accessors ------>
  //

  //
  // <------ Plugin Policy requirements ------>
  //

  @Override
  protected PluginPolicyState exportState() {
    return new PluginPolicyState<>(getClusterResource(), getNodes(), getSchedulerApplications());
  }

  private SchedulerApplication<A> moveApplicationReference(ApplicationId appId,
                                                           SchedulerApplication<? extends SchedulerApplicationAttempt> app,
                                                           String targetQueueName) throws YarnException {
    SchedulerApplicationAttempt appAttempt = app.getCurrentAppAttempt();
    String destQueueName = invokeMethod("handleMoveToPlanQueue", new Class<?>[]{String.class}, targetQueueName);
    LeafQueue dest = invokeMethod("getAndCheckLeafQueue", new Class<?>[]{String.class}, destQueueName);

    SchedulerApplication<A> newApp = new SchedulerApplication<>(dest, app.getUser());
    if (appAttempt != null) {
      //wrap attempt in appropriate implementation

      A newAppAttempt = FiCaPluginApplicationAttempt.getInstance(aClass, appAttempt, dest.getActiveUsersManager(), inner.getRMContext());
      if (newAppAttempt instanceof Configurable)
        ((Configurable) newAppAttempt).setConf(getConf());
      String sourceQueueName = newAppAttempt.getQueue().getQueueName();

      //the rest is basically standard moveApplication code from CapacityScheduler

      // Validation check - ACLs, submission limits for user & queue
      String user = newAppAttempt.getUser();
      try {
        dest.submitApplication(appId, user, destQueueName);
      } catch (AccessControlException e) {
        throw new YarnException(e);
      }
      // Move all live containers
      for (RMContainer rmContainer : newAppAttempt.getLiveContainers()) {
        // attach the Container to another queue
        dest.attachContainer(getClusterResource(), newAppAttempt, rmContainer);
      }
      // Update metrics
      newAppAttempt.move(dest);
      // Calculate its priority given the new scheduler
      updateAppPriority(newAppAttempt);
      // Submit to a new queue
      synchronized (dest) {
        // Add the attempt to our data-structures
        GeneralUtils.invokeMethod(dest, LeafQueue.class, "addApplicationAttempt",
          new Class[]{
            FiCaSchedulerApp.class,
            LeafQueue.User.class
          },
          newAppAttempt,
          dest.getUser(user));
      }
      LOG.info("App: " + appId + " successfully moved from "
        + sourceQueueName + " to: " + destQueueName);
      newApp.setCurrentAppAttempt(newAppAttempt);
    }
    return newApp;
  }

  @Override
  protected <T extends SchedulerNode & PluginSchedulerNode> void importState(PluginPolicyState<T> state) {
    synchronized (inner) {
      //transfer total resource
      writeField("clusterResource", state.getClusterResource());
      CSQueue root = readField("root");
      root.updateClusterResource(getClusterResource(), new ResourceLimits(getClusterResource()));

      //transfer nodes
      Map<NodeId, N> newNodes = readField("nodes");
      for (Map.Entry<NodeId, T> nodeEntry : state.getNodes().entrySet()) {
        T node = nodeEntry.getValue();
        newNodes.put(nodeEntry.getKey(), FiCaPluginSchedulerNode.getInstance(nClass, node));
        // update this node to node label manager
        RMNodeLabelsManager labelManager = getLabelManager();
        if (labelManager != null) {
          labelManager.activateNode(node.getNodeID(),
            node.getTotalResource());
        }
        updateMaximumAllocation(node, true);
      }

      //transfer node-related properties
      AtomicInteger numNodeManagers = readField("numNodeManagers");
      numNodeManagers.set(newNodes.size());

      //transfer applications
      Map<ApplicationId, SchedulerApplication<A>> myApps = getSchedulerApplications();
      for (Map.Entry<ApplicationId, ? extends SchedulerApplication<? extends SchedulerApplicationAttempt>> appEntry :
        state.getApplications().entrySet()) {
        ApplicationId appId = appEntry.getKey();
        SchedulerApplication<? extends SchedulerApplicationAttempt> app = appEntry.getValue();
        Queue oldQueue = app.getQueue();
        String newQueueName = resolveMoveQueue(appId, app);
        try {
          //build a new scheduler application based on the old one
          myApps.put(appId, moveApplicationReference(appId, app, newQueueName));
        } catch (Exception e) {
          throw new PosumException("Could not move " + appId.toString() +
            " from " + oldQueue.getQueueName() + " to " + newQueueName, e);
        }
      }
    }
  }

  @Override
  public boolean forceContainerAssignment(ApplicationId appId, String hostName, Priority priority) {
    N node = null;
    for (Map.Entry<NodeId, N> nodeEntry : getNodes().entrySet()) {
      if (nodeEntry.getKey().getHost().equals(hostName))
        node = nodeEntry.getValue();
    }
    if (node == null)
      throw new PosumException("Node could not be found for " + hostName);

    SchedulerApplication<A> app = getSchedulerApplications().get(appId);
    MutableObject allocatedContainer = new MutableObject();
    Resource assignedResource = GeneralUtils.invokeMethod(app.getQueue(), LeafQueue.class, "assignContainer",
      new Class[]{
        Resource.class,
        FiCaSchedulerNode.class,
        FiCaSchedulerApp.class,
        Priority.class,
        ResourceRequest.class,
        NodeType.class,
        RMContainer.class,
        MutableObject.class,
        ResourceLimits.class
      },
      getClusterResource(),
      node,
      app.getCurrentAppAttempt(),
      priority,
      ClusterUtils.createResourceRequest(getMinimumResourceCapability(), hostName, 1),
      NodeType.NODE_LOCAL,
      null,
      allocatedContainer,
      new ResourceLimits(getLabelManager().getResourceByLabel(RMNodeLabelsManager.NO_LABEL, getClusterResource())));
    if (!assignedResource.equals(Resources.none())) {
      if (allocatedContainer.getValue() != null) {
        app.getCurrentAppAttempt().incNumAllocatedContainers(NodeType.NODE_LOCAL, NodeType.NODE_LOCAL);
      }
      allocateResources(app.getCurrentAppAttempt(), (LeafQueue) app.getQueue(), assignedResource, node.getLabels());
      return true;
    }
    return false;
  }

  private void allocateResources(A appAttempt, LeafQueue queue, Resource assignedResource, Set<String> nodeLabels) {
    GeneralUtils.invokeMethod(queue, LeafQueue.class,
      "allocateResource",
      new Class[]{
        Resource.class,
        SchedulerApplicationAttempt.class,
        Resource.class,
        Set.class
      },
      getClusterResource(),
      appAttempt,
      assignedResource,
      nodeLabels
    );
    if (queue.getParent() != null)
      allocateResources((ParentQueue) queue.getParent(), assignedResource, nodeLabels);
  }

  private void allocateResources(ParentQueue queue, Resource assignedResource, Set<String> nodeLabels) {
    GeneralUtils.invokeMethod(queue, AbstractCSQueue.class,
      "allocateResource",
      new Class[]{
        Resource.class,
        Resource.class,
        Set.class
      },
      getClusterResource(),
      assignedResource,
      nodeLabels
    );
    if (queue.getParent() != null)
      allocateResources((ParentQueue) queue.getParent(), assignedResource, nodeLabels);
  }

  @Override
  public Map<String, SchedulerNodeReport> getNodeReports() {
    Map<String, SchedulerNodeReport> reports = new HashMap<>(getNumClusterNodes());
    for (NodeId nodeId : getNodes().keySet()) {
      reports.put(nodeId.getHost(), getNodeReport(nodeId));
    }
    return reports;
  }

  //
  // <------ /  Plugin Policy requirements ------>
  //
}

