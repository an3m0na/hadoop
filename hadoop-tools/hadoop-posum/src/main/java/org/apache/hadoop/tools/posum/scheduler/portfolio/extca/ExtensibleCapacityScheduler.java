package org.apache.hadoop.tools.posum.scheduler.portfolio.extca;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.tools.posum.common.util.PosumException;
import org.apache.hadoop.tools.posum.common.util.Utils;
import org.apache.hadoop.tools.posum.scheduler.portfolio.PluginPolicy;
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
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceOption;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class ExtensibleCapacityScheduler<
  A extends ExtCaAppAttempt,
  N extends ExtCaSchedulerNode>
  extends PluginPolicy<A, N> implements PreemptableResourceScheduler, CapacitySchedulerContext {

  private static Log LOG = LogFactory.getLog(ExtensibleCapacityScheduler.class);

  private static final RecordFactory recordFactory =
    RecordFactoryProvider.getRecordFactory(null);
  private CapacitySchedulerConfiguration capacityConf;
  private final boolean customConf;
  protected final CapacityScheduler inner;

  public ExtensibleCapacityScheduler(Class<A> aClass,
                                     Class<N> nClass,
                                     String policyName,
                                     boolean customConf) {
    super(aClass, nClass, policyName);
    this.inner = new CapacityScheduler();
    this.customConf = customConf;
  }

  //
  // <------ Helper reflection methods for accessing internal CapacityScheduler methods and fields ------>
  //

  protected void writeField(String name, Object value) {
    Utils.writeField(inner, CapacityScheduler.class, name, value);
  }

  protected <T> T readField(String name) {
    return Utils.readField(inner, CapacityScheduler.class, name);
  }


  protected <T> T invokeMethod(String name, Class<?>[] paramTypes, Object... args) {
    return Utils.invokeMethod(inner, CapacityScheduler.class, name, paramTypes, args);
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
   * @param queue         the name of the application's queue in the other scheduler
   * @param applicationId the application id
   * @return the target queue name for this scheduler
   */
  protected String resolveMoveQueue(String queue,
                                    ApplicationId applicationId,
                                    String user) {
    //TODO
    return YarnConfiguration.DEFAULT_QUEUE_NAME;
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
   * Override this to apply priority updates to a leaf queue's application set.
   * By default, each instance of A is passed to updateAppPriority(A) in order
   *
   * @param queue              target leaf queue
   * @param applicationSetName either "pendingApplications" or "activeApplications"
   */
  protected void updateApplicationPriorities(LeafQueue queue, String applicationSetName) {
    Set<FiCaSchedulerApp> apps = Utils.readField(queue, LeafQueue.class, applicationSetName);
    List<A> removedApps = new ArrayList<>(apps.size());
    for (Iterator<FiCaSchedulerApp> i = apps.iterator(); i.hasNext(); ) {
      A app = (A) i.next();
      // remove, update and remember
      i.remove();
      updateAppPriority(app);
      removedApps.add(app);
    }
    // add everything back in order to re-sort
    for (A app : removedApps) {
      apps.add(app);
    }
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
      LeafQueue leaf = (LeafQueue) queue;
      updateApplicationPriorities(leaf, "pendingApplications");
      updateApplicationPriorities(leaf, "activeApplications");
    } else {
      for (CSQueue csQueue : queue.getChildQueues()) {
        updateApplicationPriorities(csQueue);
      }
    }
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

      A attempt =
        ExtCaAppAttempt.getInstance(aClass, pluginConf, applicationAttemptId, application.getUser(),
          queue, queue.getActiveUsersManager(), inner.getRMContext());
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
      N schedulerNode = ExtCaSchedulerNode.getInstance(nClass, nodeManager,
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

  public SchedulerApplication<A> moveApplicationReference(ApplicationId appId,
                                                          SchedulerApplication<? extends ExtCaAppAttempt> app,
                                                          LeafQueue source,
                                                          String targetQueueName) throws YarnException {
    ExtCaAppAttempt appAttempt = app.getCurrentAppAttempt();
    String destQueueName = invokeMethod("handleMoveToPlanQueue", new Class<?>[]{String.class}, targetQueueName);
    LeafQueue dest = invokeMethod("getAndCheckLeafQueue", new Class<?>[]{String.class}, destQueueName);

    SchedulerApplication<A> newApp = new SchedulerApplication<>(dest, app.getUser());

    if (appAttempt != null) {
      //wrap attempt in appropriate implementation

      A newAppAttempt = ExtCaAppAttempt.getInstance(aClass, appAttempt);
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
        source.detachContainer(getClusterResource(), newAppAttempt, rmContainer);
        // attach the Container to another queue
        dest.attachContainer(getClusterResource(), newAppAttempt, rmContainer);
      }
      // Detach the application..
      source.finishApplicationAttempt(newAppAttempt, sourceQueueName);
      source.getParent().finishApplication(appId, newAppAttempt.getUser());
      // Finish app & addSource metrics
      newAppAttempt.move(dest);
      // Calculate its priority given the new scheduler
      updateAppPriority(newAppAttempt);
      // Submit to a new queue
      dest.submitApplicationAttempt(newAppAttempt, user);
      LOG.info("App: " + appId + " successfully moved from "
        + sourceQueueName + " to: " + destQueueName);
      newApp.setCurrentAppAttempt(newAppAttempt);
    }

    return newApp;
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
      invokeMethod("initializeQueues", new Class<?>[]{CapacitySchedulerConfiguration.class}, capacityConf);
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
    if (customConf) {
      // reinitialize with the given capacity scheduler configuration
      capacityConf = loadCustomCapacityConf(conf);
      initializeWithModifiedConf();
    } else {
      capacityConf = getConfiguration();
    }
  }

  @Override
  public void reinitialize(Configuration conf, RMContext rmContext) throws IOException {
    // this will reinitialize with default capacity scheduler configuration found in capacity-scheduler.xml
    inner.reinitialize(conf, rmContext);
    if (customConf) {
      // reinitialize with the given capacity scheduler configuration
      capacityConf = loadCustomCapacityConf(conf);
      writeField("conf", capacityConf);
      reinitializeWithModifiedConf();
    } else {
      capacityConf = getConfiguration();
    }
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
    return ExtCaAppAttempt.getInstance(aClass, (ExtCaAppAttempt) inner.getApplicationAttempt(applicationAttemptId));
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
        Utils.readField(leaf, LeafQueue.class, "pendingApplications");
      builder.append(" --pending:\n");
      for (A app : pendingApplications) {
        builder.append("   ").append(app);
      }
      Set<A> activeApplications =
        Utils.readField(leaf, LeafQueue.class, "activeApplications");
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


  public void transferStateFromPolicy(ExtensibleCapacityScheduler<ExtCaAppAttempt, ExtCaSchedulerNode> otherExtCa) {
    synchronized (inner) {
      //transfer total resource
      writeField("clusterResource", otherExtCa.getClusterResource());
      CSQueue root = readField("root");
      root.updateClusterResource(getClusterResource(), new ResourceLimits(getClusterResource()));

      //transfer nodes
      Map<NodeId, N> newNodes = readField("nodes");
      for (Map.Entry<NodeId, ExtCaSchedulerNode> nodeEntry : otherExtCa.getNodes().entrySet()) {
        ExtCaSchedulerNode node = nodeEntry.getValue();
        newNodes.put(nodeEntry.getKey(), ExtCaSchedulerNode.getInstance(nClass, node));
        updateMaximumAllocation(node, true);
      }

      //transfer node-related properties
      writeField("labelManager", otherExtCa.getLabelManager());
      AtomicInteger numNodeManagers = readField("numNodeManagers");
      numNodeManagers.set(otherExtCa.getNumClusterNodes());

      //transfer applications
      Map<ApplicationId, SchedulerApplication<A>> myApps = getSchedulerApplications();
      Map<ApplicationId, ? extends SchedulerApplication<? extends ExtCaAppAttempt>> othersApps =
        otherExtCa.getSchedulerApplications();
      for (Map.Entry<ApplicationId, ? extends SchedulerApplication<? extends ExtCaAppAttempt>> appEntry :
        othersApps.entrySet()) {
        ApplicationId appId = appEntry.getKey();
        SchedulerApplication<? extends ExtCaAppAttempt> app = appEntry.getValue();
        LeafQueue oldQueue = (LeafQueue) app.getQueue();
        String newQueueName = resolveMoveQueue(oldQueue.getQueuePath(), appId, app.getUser());
        try {
          //build a new scheduler application based on the old one
          myApps.put(appId, moveApplicationReference(appId, app, oldQueue, newQueueName));
        } catch (Exception e) {
          throw new PosumException("Could not move " + appId.toString() +
            " from " + oldQueue.getQueuePath() + " to " + newQueueName, e);
        }
      }
    }

  }

  @Override
  public void transferStateFromPolicy(PluginPolicy other) {
    if (PluginPolicy.class.isAssignableFrom(ExtensibleCapacityScheduler.class)) {
      transferStateFromPolicy((ExtensibleCapacityScheduler) other);
      return;
    }
    //TODO
    throw new PosumException("Cannot transfer state from unknown policy " + other.getClass().getName());
  }

  //
  // <------ /  Plugin Policy requirements ------>
  //
}

