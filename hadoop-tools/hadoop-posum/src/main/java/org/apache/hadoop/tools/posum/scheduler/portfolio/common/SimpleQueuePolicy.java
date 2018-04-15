package org.apache.hadoop.tools.posum.scheduler.portfolio.common;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.tools.posum.common.util.PosumException;
import org.apache.hadoop.tools.posum.common.util.cluster.ClusterUtils;
import org.apache.hadoop.tools.posum.common.util.communication.DatabaseProvider;
import org.apache.hadoop.tools.posum.common.util.conf.PosumConfiguration;
import org.apache.hadoop.tools.posum.scheduler.portfolio.PluginPolicy;
import org.apache.hadoop.tools.posum.scheduler.portfolio.PluginPolicyState;
import org.apache.hadoop.tools.posum.scheduler.portfolio.PluginSchedulerNode;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.UpdatedContainerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerAppUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplication;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNodeReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.ContainerExpiredSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeResourceUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

public abstract class SimpleQueuePolicy<
  A extends FiCaPluginApplicationAttempt,
  N extends FiCaPluginSchedulerNode,
  Q extends SimpleQueue,
  S extends SimpleQueuePolicy<A, N, Q, S>>
  extends PluginPolicy<A, N> {

  private Configuration conf;
  private static final String DEFAULT_QUEUE_NAME = "default";
  private static final RecordFactory recordFactory =
    RecordFactoryProvider.getRecordFactory(null);

  protected Resource usedResource = recordFactory.newRecordInstance(Resource.class);
  private boolean usePortForNodeName;
  private final ResourceCalculator resourceCalculator = new DefaultResourceCalculator();

  protected Q queue;
  private Class<Q> qClass;
  protected ConcurrentSkipListSet<A> orderedApps;
  protected Resource usedAMResource = Resource.newInstance(0, 0);
  protected float maxAMRatio;

  public SimpleQueuePolicy(Class<A> aClass, Class<N> nClass, Class<Q> qClass, Class<S> sClass) {
    super(aClass, nClass, sClass.getName());
    this.qClass = qClass;
  }

  public Resource getUsedResource() {
    return usedResource;
  }

  protected void validateConf(Configuration conf) {
    // Just like FIFO
    // validate scheduler memory allocation setting
    int minMem = conf.getInt(
      YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
      YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB);
    int maxMem = conf.getInt(
      YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
      YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB);

    if (minMem <= 0 || minMem > maxMem) {
      throw new YarnRuntimeException("Invalid resource scheduler memory"
        + " allocation configuration"
        + ", " + YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB
        + "=" + minMem
        + ", " + YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB
        + "=" + maxMem + ", min and max should be greater than 0"
        + ", max should be no smaller than min.");
    }
  }

  /**
   * Override to change the order of applications in the orderedApps collection.
   * By default, FIFO is applied.
   */
  protected Comparator<A> getApplicationComparator() {
    return new Comparator<A>() {
      @Override
      public int compare(A o1, A o2) {
        return o1.getApplicationId().compareTo(o2.getApplicationId());
      }
    };
  }

  protected synchronized void initScheduler(Configuration conf) {
    validateConf(conf);
    this.conf = conf;
    //General allocation configs found in FIFO and FS
    this.minimumAllocation =
      Resources.createResource(conf.getInt(
        YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB));
    Resource maxResource = Resources.createResource(conf.getInt(
      YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
      YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB),
      conf.getInt(
        YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES));
    initMaximumResourceCapability(maxResource);
    this.usePortForNodeName = conf.getBoolean(
      YarnConfiguration.RM_SCHEDULER_INCLUDE_PORT_IN_NODE_NAME,
      YarnConfiguration.DEFAULT_RM_SCHEDULER_USE_PORT_FOR_NODE_NAME);
    this.applications = new ConcurrentHashMap<>();
    this.orderedApps = new ConcurrentSkipListSet<>(getApplicationComparator());
    this.queue = SimpleQueue.getInstance(qClass, DEFAULT_QUEUE_NAME, this);
  }

  @Override
  public void initializePlugin(Configuration conf, DatabaseProvider dbProvider) {
    super.initializePlugin(conf, dbProvider);
    this.maxAMRatio = conf.getFloat(PosumConfiguration.MAX_AM_RATIO, PosumConfiguration.MAX_AM_RATIO_DEFAULT);
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    initScheduler(conf);
    super.serviceInit(conf);
  }

  @Override
  protected void completedContainer(RMContainer rmContainer,
                                    ContainerStatus containerStatus,
                                    RMContainerEventType rmContainerEventType) {

    if (rmContainer == null) {
      logger.trace("Null container completed...");
      return;
    }

    // Get the application for the finished container
    Container container = rmContainer.getContainer();
    A application =
      getCurrentAttemptForContainer(container.getId());
    ApplicationId appId =
      container.getId().getApplicationAttemptId().getApplicationId();

    // Get the node on which the container was allocated
    N node = nodes.get(container.getNodeId());

    if (application == null) {
      logger.trace("Unknown application: " + appId +
        " released container " + container.getId() +
        " on node: " + node +
        " with event: " + rmContainerEventType);
      return;
    }

    // Inform the application
    application.containerCompleted(rmContainer, containerStatus, rmContainerEventType);

    // Inform the node
    node.releaseContainer(container);

    // Update total usage
    Resources.subtractFrom(usedResource, container.getResource());

    logger.trace("Application attempt " + application.getApplicationAttemptId() +
      " released container " + container.getId() +
      " on node: " + node +
      " with event: " + rmContainerEventType);
  }

  @Override
  public void setConf(Configuration configuration) {
    conf = configuration;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setRMContext(RMContext rmContext) {
    this.rmContext = rmContext;
  }

  @Override
  public void reinitialize(Configuration configuration, RMContext rmContext) throws IOException {
    setConf(conf);
  }

  @Override
  public void recover(RMStateStore.RMState rmState) throws Exception {
    // not implemented anywhere
  }

  @Override
  public QueueInfo getQueueInfo(String queueName, boolean includeChildQueues,
                                boolean recursive) {
    return queue.getQueueInfo(includeChildQueues, recursive);
  }

  @Override
  public List<QueueUserACLInfo> getQueueUserAclInfo() {
    return queue.getQueueUserAclInfo(null);
  }

  @Override
  public ResourceCalculator getResourceCalculator() {
    return resourceCalculator;
  }

  @Override
  public int getNumClusterNodes() {
    return nodes.size();
  }

  @Override
  public Allocation allocate(
    ApplicationAttemptId applicationAttemptId, List<ResourceRequest> ask,
    List<ContainerId> release, List<String> blacklistAdditions, List<String> blacklistRemovals) {

    //Same everywhere

    A application = getApplicationAttempt(applicationAttemptId);
    if (application == null) {
      logger.error("Calling allocate on removed " +
        "or non existant application " + applicationAttemptId);
      return EMPTY_ALLOCATION;
    }

    // Sanity check
    SchedulerUtils.normalizeRequests(ask, resourceCalculator,
      clusterResource, minimumAllocation, getMaximumResourceCapability());

    // Release containers
    releaseContainers(release, application);

    synchronized (application) {

      // make sure we aren't stopping/removing the application
      // when the allocate comes in
      if (application.isStopped()) {
        logger.info("Calling allocate on a stopped " +
          "application " + applicationAttemptId);
        return EMPTY_ALLOCATION;
      }

      if (!ask.isEmpty()) {
        logger.trace("allocate: pre-update" +
          " applicationId=" + applicationAttemptId +
          " application=" + application);
        application.showRequests();

        // Update application requests
        application.updateResourceRequests(ask);

        logger.trace("allocate: post-update" +
          " applicationId=" + applicationAttemptId +
          " application=" + application);
        application.showRequests();

        logger.trace("allocate:" +
          " applicationId=" + applicationAttemptId +
          " #ask=" + ask.size());
      }

      application.updateBlacklist(blacklistAdditions, blacklistRemovals);
      SchedulerApplicationAttempt.ContainersAndNMTokensAllocation allocation =
        application.pullNewlyAllocatedContainersAndNMTokens();
      Resource headroom = application.getHeadroom();
      application.setApplicationHeadroomForMetrics(headroom);
      return new Allocation(allocation.getContainerList(), headroom, null,
        null, null, allocation.getNMTokenList());
    }
  }

  @Override
  public QueueMetrics getRootQueueMetrics() {
    return queue.getMetrics();
  }

  @Override
  public boolean checkAccess(UserGroupInformation userGroupInformation, QueueACL queueACL, String queueName) {
    return queue.hasAccess(queueACL, userGroupInformation);
  }

  @Override
  public List<ApplicationAttemptId> getAppsInQueue(String queueName) {
    if (queueName.equals(queue.getQueueName())) {
      List<ApplicationAttemptId> attempts =
        new ArrayList<ApplicationAttemptId>(applications.size());
      for (SchedulerApplication<A> app : applications.values()) {
        attempts.add(app.getCurrentAppAttempt().getApplicationAttemptId());
      }
      return attempts;
    } else {
      return null;
    }
  }

  @Override
  public void handle(SchedulerEvent event) {
    super.handle(event);
    switch (event.getType()) {
      case NODE_ADDED:
        if (!(event instanceof NodeAddedSchedulerEvent)) {
          throw new RuntimeException("Unexpected event type: " + event);
        }
        NodeAddedSchedulerEvent nodeAddedEvent = (NodeAddedSchedulerEvent) event;
        addNode(nodeAddedEvent.getAddedRMNode());
        recoverContainersOnNode(nodeAddedEvent.getContainerReports(),
          nodeAddedEvent.getAddedRMNode());
        break;
      case NODE_REMOVED:
        if (!(event instanceof NodeRemovedSchedulerEvent)) {
          throw new RuntimeException("Unexpected event type: " + event);
        }
        NodeRemovedSchedulerEvent nodeRemovedEvent = (NodeRemovedSchedulerEvent) event;
        removeNode(nodeRemovedEvent.getRemovedRMNode());
        break;
      case NODE_UPDATE:
        if (!(event instanceof NodeUpdateSchedulerEvent)) {
          throw new RuntimeException("Unexpected event type: " + event);
        }
        NodeUpdateSchedulerEvent nodeUpdatedEvent = (NodeUpdateSchedulerEvent) event;
        nodeUpdate(nodeUpdatedEvent.getRMNode());
        break;
      case NODE_RESOURCE_UPDATE: {
        if (!(event instanceof NodeResourceUpdateSchedulerEvent)) {
          throw new RuntimeException("Unexpected event type: " + event);
        }
        NodeResourceUpdateSchedulerEvent nodeResourceUpdatedEvent =
          (NodeResourceUpdateSchedulerEvent) event;
        updateNodeResource(nodeResourceUpdatedEvent.getRMNode(),
          nodeResourceUpdatedEvent.getResourceOption());
      }
      break;
      case APP_ADDED:
        if (!(event instanceof AppAddedSchedulerEvent)) {
          throw new RuntimeException("Unexpected event type: " + event);
        }
        AppAddedSchedulerEvent appAddedEvent = (AppAddedSchedulerEvent) event;

        addApplication(appAddedEvent.getApplicationId(), appAddedEvent.getUser(),
          appAddedEvent.getIsAppRecovering());
        break;
      case APP_REMOVED:
        if (!(event instanceof AppRemovedSchedulerEvent)) {
          throw new RuntimeException("Unexpected event type: " + event);
        }
        AppRemovedSchedulerEvent appRemovedEvent = (AppRemovedSchedulerEvent) event;
        doneApplication(appRemovedEvent.getApplicationID(),
          appRemovedEvent.getFinalState());
        break;
      case APP_ATTEMPT_ADDED:
        if (!(event instanceof AppAttemptAddedSchedulerEvent)) {
          throw new RuntimeException("Unexpected event type: " + event);
        }
        AppAttemptAddedSchedulerEvent appAttemptAddedEvent =
          (AppAttemptAddedSchedulerEvent) event;
        addApplicationAttempt(appAttemptAddedEvent.getApplicationAttemptId(),
          appAttemptAddedEvent.getTransferStateFromPreviousAttempt(),
          appAttemptAddedEvent.getIsAttemptRecovering());
        break;
      case APP_ATTEMPT_REMOVED:
        if (!(event instanceof AppAttemptRemovedSchedulerEvent)) {
          throw new RuntimeException("Unexpected event type: " + event);
        }
        AppAttemptRemovedSchedulerEvent appAttemptRemovedEvent =
          (AppAttemptRemovedSchedulerEvent) event;
        try {
          doneApplicationAttempt(
            appAttemptRemovedEvent.getApplicationAttemptID(),
            appAttemptRemovedEvent.getFinalAttemptState(),
            appAttemptRemovedEvent.getKeepContainersAcrossAppAttempts());
        } catch (IOException ie) {
          logger.error("Unable to remove application "
            + appAttemptRemovedEvent.getApplicationAttemptID(), ie);
        }
        break;
      case CONTAINER_EXPIRED:
        if (!(event instanceof ContainerExpiredSchedulerEvent)) {
          throw new RuntimeException("Unexpected event type: " + event);
        }
        ContainerExpiredSchedulerEvent containerExpiredEvent =
          (ContainerExpiredSchedulerEvent) event;
        ContainerId containerId = containerExpiredEvent.getContainerId();
        completedContainer(getRMContainer(containerId),
          SchedulerUtils.createAbnormalContainerStatus(
            containerId,
            SchedulerUtils.EXPIRED_CONTAINER),
          RMContainerEventType.EXPIRE);
        break;
      //TODO (only if preemptable capacity scheduler) case NODE_LABELS_UPDATE:
      default:
        logger.error("Unknown event arrived at scheduler:" + event.toString());
    }
  }

  private void doneApplicationAttempt(ApplicationAttemptId appAttemptId,
                                      RMAppAttemptState finalAttemptState,
                                      boolean keepContainers)
    throws IOException {

    //Just like FIFO

    A attempt = getApplicationAttempt(appAttemptId);
    SchedulerApplication<A> application =
      applications.get(appAttemptId.getApplicationId());
    if (application == null || attempt == null) {
      throw new IOException("Unknown application " + appAttemptId +
        " has completed!");
    }

    onAppAttemptDone(attempt);

    // Kill all 'live' containers
    for (RMContainer container : attempt.getLiveContainers()) {
      if (keepContainers
        && container.getState().equals(RMContainerState.RUNNING)) {
        // do not kill the running container in the case of work-preserving AM
        // restart.
        logger.info("Skip killing " + container.getContainerId());
        continue;
      }
      completedContainer(container,
        SchedulerUtils.createAbnormalContainerStatus(
          container.getContainerId(), SchedulerUtils.COMPLETED_APPLICATION),
        RMContainerEventType.KILL);
    }
    // Fair also releases reserved containers

    // Clean up pending requests, metrics etc.
    attempt.stop(finalAttemptState);
  }

  private void addApplicationAttempt(ApplicationAttemptId appAttemptId, boolean transferStateFromPreviousAttempt, boolean isAttemptRecovering) {
    SchedulerApplication<A> application =
      applications.get(appAttemptId.getApplicationId());
    String user = application.getUser();
    A schedulerAppAttempt = FiCaPluginApplicationAttempt.getInstance(aClass, appAttemptId, user, queue, new ActiveUsersManager(queue.getMetrics()), this.rmContext);
    if (schedulerAppAttempt instanceof Configurable)
      ((Configurable) schedulerAppAttempt).setConf(conf);

    if (transferStateFromPreviousAttempt) {
      schedulerAppAttempt.transferStateFromPreviousAttempt(application.getCurrentAppAttempt());
    }

    application.setCurrentAppAttempt(schedulerAppAttempt);
    onAppAttemptAdded(schedulerAppAttempt);

    queue.getMetrics().submitAppAttempt(user);
    logger.trace("Added Application Attempt " + appAttemptId
      + " to scheduler from user " + application.getUser());
    if (isAttemptRecovering) {
      if (logger.isTraceEnabled()) {
        logger.trace(appAttemptId
          + " is recovering. Skipping notifying ATTEMPT_ADDED");
      }
    } else {
      rmContext.getDispatcher().getEventHandler().handle(
        new RMAppAttemptEvent(appAttemptId,
          RMAppAttemptEventType.ATTEMPT_ADDED));
    }
  }

  private void doneApplication(ApplicationId applicationId, RMAppState finalState) {
    SchedulerApplication<A> application =
      applications.get(applicationId);
    if (application == null) {
      logger.warn("Couldn't find application " + applicationId);
      return;
    }

    // Inform the activeUsersManager
    queue.deactivateApplication(application.getUser(),
      applicationId);
    application.stop(finalState);
    applications.remove(applicationId);
  }

  private void addApplication(ApplicationId applicationId, String user, boolean isAppRecovering) {
    SchedulerApplication<A> application =
      new SchedulerApplication<>(queue, user);
    applications.put(applicationId, application);

    queue.getMetrics().submitApp(user);
    logger.info("Accepted application " + applicationId + " from user: " + user
      + ", currently num of applications: " + applications.size());
    if (isAppRecovering) {
      if (logger.isTraceEnabled()) {
        logger.trace(applicationId + " is recovering. Skip notifying APP_ACCEPTED");
      }
    } else {
      rmContext.getDispatcher().getEventHandler()
        .handle(new RMAppEvent(applicationId, RMAppEventType.APP_ACCEPTED));
    }
  }

  private void nodeUpdate(RMNode rmNode) {

    // Just like FIFO

    N node = nodes.get(rmNode.getNodeID());

    List<UpdatedContainerInfo> containerInfoList = rmNode.pullContainerUpdates();
    List<ContainerStatus> newlyLaunchedContainers = new ArrayList<ContainerStatus>();
    List<ContainerStatus> completedContainers = new ArrayList<ContainerStatus>();
    for (UpdatedContainerInfo containerInfo : containerInfoList) {
      newlyLaunchedContainers.addAll(containerInfo.getNewlyLaunchedContainers());
      completedContainers.addAll(containerInfo.getCompletedContainers());
    }
    // Processing the newly launched containers
    for (ContainerStatus launchedContainer : newlyLaunchedContainers) {
      containerLaunchedOnNode(launchedContainer.getContainerId(), node);
    }

    // Process completed containers
    for (ContainerStatus completedContainer : completedContainers) {
      ContainerId containerId = completedContainer.getContainerId();
      logger.trace("Container FINISHED: " + containerId);
      completedContainer(getRMContainer(containerId),
        completedContainer, RMContainerEventType.FINISHED);
    }

    if (Resources.greaterThanOrEqual(resourceCalculator, clusterResource,
      node.getAvailableResource(), minimumAllocation)) {
      logger.trace("Node heartbeat " + rmNode.getNodeID() +
        " available resource = " + node.getAvailableResource());

      assignContainers(node);

      logger.trace("Node after allocation " + rmNode.getNodeID() + " resource = "
        + node.getAvailableResource());
    }

    queue.setAvailableResourcesToQueue(Resources.subtract(clusterResource,
      usedResource));
  }

  /**
   * Override to add custom logic for parsing the application queue and assigning containers.
   *
   * @param node node on which resources are available to be allocated
   */
  protected void assignFromQueue(N node) {

    // Just like FIFO

    for (A app : orderedApps) {
      logger.trace("pre-assignContainers");
      app.showRequests();
      synchronized (app) {
        // Check if this resource is on the blacklist
        if (SchedulerAppUtils.isBlacklisted(app, node, logger)) {
          continue;
        }

        boolean amNotStarted = !hasAMResources(app);
        if (amNotStarted && !canAMStart(app)) {
          continue;
        }

        for (Priority priority : app.getPriorities()) {
          int maxContainers =
            getMaxAllocatableContainers(app, priority, node,
              NodeType.OFF_SWITCH);
          // Ensure the app needs containers of this priority
          if (maxContainers > 0) {
            int assignedContainers =
              assignContainersOnNode(node, app, priority);
            // Do not assign out of order w.r.t priorities
            if (assignedContainers == 0)
              break;
            else if (amNotStarted && hasAMResources(app))
              Resources.addTo(usedAMResource, app.getAMResource());
          }
        }
      }

      logger.trace("post-assignContainers");
      app.showRequests();

      // Done
      if (Resources.lessThan(getResourceCalculator(), clusterResource,
        node.getAvailableResource(), minimumAllocation)) {
        break;
      }
    }
  }

  protected boolean hasAMResources(A app) {
    return Resources.greaterThanOrEqual(getResourceCalculator(), clusterResource,
      app.getCurrentConsumption(), app.getAMResource());
  }

  protected boolean canAMStart(A app) {
    Resource amIfStarted = Resources.add(usedAMResource, app.getAMResource());
    float ratioIfStarted = Resources.divide(resourceCalculator, clusterResource, amIfStarted, clusterResource);
    return ratioIfStarted <= maxAMRatio;
  }

  /**
   * Heart of the scheduler...
   *
   * @param node node on which resources are available to be allocated
   */
  private void assignContainers(N node) {

    logger.trace("assignContainers:" +
      " node=" + node.getRMNode().getNodeAddress() +
      " #applications=" + applications.size());

    if (checkIfPrioritiesExpired()) {
      updateApplicationPriorities();
    }
    if (logger.isTraceEnabled())
      printQueue();

    assignFromQueue(node);

    // Update the applications' headroom to correctly take into
    // account the containers assigned in this update.
    for (SchedulerApplication<A> application : applications.values()) {
      A attempt = application.getCurrentAppAttempt();
      if (attempt == null) {
        continue;
      }
      updateAppHeadRoom(attempt);
    }
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
   * Override this to apply priority updates to queue's application set.
   * By default, each instance of A is passed to updateAppPriority(A) in order
   */
  protected synchronized void updateApplicationPriorities() {
    ConcurrentSkipListSet<A> oldOrderedApps = this.orderedApps;
    ConcurrentSkipListSet<A> newOrderedApps = new ConcurrentSkipListSet<>(getApplicationComparator());
    for (A app : oldOrderedApps) {
      updateAppPriority(app);
      newOrderedApps.add(app);
    }
    this.orderedApps = newOrderedApps;
  }

  public int getMaxAllocatableContainers(A application,
                                         Priority priority, N node, NodeType type) {
    int maxContainers = 0;

    ResourceRequest offSwitchRequest =
      application.getResourceRequest(priority, ResourceRequest.ANY);
    if (offSwitchRequest != null) {
      maxContainers = offSwitchRequest.getNumContainers();
    }

    if (type == NodeType.OFF_SWITCH) {
      return maxContainers;
    }

    if (type == NodeType.RACK_LOCAL) {
      ResourceRequest rackLocalRequest =
        application.getResourceRequest(priority, node.getRMNode().getRackName());
      if (rackLocalRequest == null) {
        return maxContainers;
      }

      maxContainers = Math.min(maxContainers, rackLocalRequest.getNumContainers());
    }

    if (type == NodeType.NODE_LOCAL) {
      ResourceRequest nodeLocalRequest =
        application.getResourceRequest(priority, node.getRMNode().getNodeAddress());
      if (nodeLocalRequest != null) {
        maxContainers = Math.min(maxContainers, nodeLocalRequest.getNumContainers());
      }
    }

    return maxContainers;
  }


  public int assignContainersOnNode(N node,
                                    A application, Priority priority
  ) {
    // Data-local
    int nodeLocalContainers =
      assignNodeLocalContainers(node, application, priority);

    // Rack-local
    int rackLocalContainers =
      assignRackLocalContainers(node, application, priority);

    // Off-switch
    int offSwitchContainers =
      assignOffSwitchContainers(node, application, priority);


    logger.trace("assignContainersOnNode:" +
      " node=" + node.getRMNode().getNodeAddress() +
      " application=" + application.getApplicationId().getId() +
      " priority=" + priority.getPriority() +
      " #assigned=" +
      (nodeLocalContainers + rackLocalContainers + offSwitchContainers));


    return (nodeLocalContainers + rackLocalContainers + offSwitchContainers);
  }

  protected int assignNodeLocalContainers(N node,
                                          A application, Priority priority) {
    int assignedContainers = 0;
    ResourceRequest request =
      application.getResourceRequest(priority, node.getNodeName());
    if (request != null) {
      // Don't allocate on this node if we don't need containers on this rack
      ResourceRequest rackRequest =
        application.getResourceRequest(priority,
          node.getRMNode().getRackName());
      if (rackRequest == null || rackRequest.getNumContainers() <= 0) {
        return 0;
      }

      int assignableContainers =
        Math.min(
          getMaxAllocatableContainers(application, priority, node,
            NodeType.NODE_LOCAL),
          request.getNumContainers());
      assignedContainers =
        assignContainer(node, application, priority,
          assignableContainers, request, NodeType.NODE_LOCAL);
    }
    return assignedContainers;
  }

  protected int assignRackLocalContainers(N node,
                                          A application, Priority priority) {
    int assignedContainers = 0;
    ResourceRequest request =
      application.getResourceRequest(priority, node.getRMNode().getRackName());
    if (request != null) {
      // Don't allocate on this rack if the application doens't need containers
      ResourceRequest offSwitchRequest =
        application.getResourceRequest(priority, ResourceRequest.ANY);
      if (offSwitchRequest.getNumContainers() <= 0) {
        return 0;
      }

      int assignableContainers =
        Math.min(
          getMaxAllocatableContainers(application, priority, node,
            NodeType.RACK_LOCAL),
          request.getNumContainers());
      assignedContainers =
        assignContainer(node, application, priority,
          assignableContainers, request, NodeType.RACK_LOCAL);
    }
    return assignedContainers;
  }

  protected int assignOffSwitchContainers(N node,
                                          A application, Priority priority) {
    int assignedContainers = 0;
    ResourceRequest request =
      application.getResourceRequest(priority, ResourceRequest.ANY);
    if (request != null) {
      assignedContainers =
        assignContainer(node, application, priority,
          request.getNumContainers(), request, NodeType.OFF_SWITCH);
    }
    return assignedContainers;
  }

  private int assignContainer(N node, A application,
                              Priority priority, int assignableContainers,
                              ResourceRequest request, NodeType type) {
    logger.trace("assignContainers:" +
      " node=" + node.getRMNode().getNodeAddress() +
      " application=" + application.getApplicationId().getId() +
      " priority=" + priority.getPriority() +
      " assignableContainers=" + assignableContainers +
      " request=" + request + " type=" + type);
    Resource capability = request.getCapability();

    int availableContainers =
      node.getAvailableResource().getMemory() / capability.getMemory(); // TODO: A buggy
    // application
    // with this
    // zero would
    // crash the
    // scheduler.
    int assignedContainers =
      Math.min(assignableContainers, availableContainers);

    if (assignedContainers > 0) {
      for (int i = 0; i < assignedContainers; ++i) {

        NodeId nodeId = node.getRMNode().getNodeID();
        ContainerId containerId = BuilderUtils.newContainerId(application
          .getApplicationAttemptId(), application.getNewContainerId());

        // Create the container
        Container container =
          BuilderUtils.newContainer(containerId, nodeId, node.getRMNode()
            .getHttpAddress(), capability, priority, null);

        // Allocate!

        // Inform the application
        RMContainer rmContainer =
          application.allocate(type, node, priority, request, container);

        // Inform the node
        node.allocateContainer(rmContainer);

        // Update usage for this container
        increaseUsedResources(rmContainer);
      }

    }

    return assignedContainers;
  }

  protected void addNode(RMNode rmNode) {
    N schedulerNode = FiCaPluginSchedulerNode.getInstance(nClass, rmNode, usePortForNodeName, rmNode.getNodeLabels());
    this.nodes.put(rmNode.getNodeID(), schedulerNode);
    Resources.addTo(clusterResource, rmNode.getTotalCapability());
    updateMaximumAllocation(schedulerNode, true);
  }

  private synchronized void removeNode(RMNode rmNode) {

    // Generally the same for everybody

    N node = nodes.get(rmNode.getNodeID());
    if (node == null) {
      return;
    }
    // Kill running containers
    for (RMContainer container : node.getRunningContainers()) {
      completedContainer(container,
        SchedulerUtils.createAbnormalContainerStatus(
          container.getContainerId(),
          SchedulerUtils.LOST_CONTAINER),
        RMContainerEventType.KILL);
    }

    //For some reason FIFO does not do this

    // Remove reservations, if any
    RMContainer reservedContainer = node.getReservedContainer();
    if (reservedContainer != null) {
      completedContainer(reservedContainer,
        SchedulerUtils.createAbnormalContainerStatus(
          reservedContainer.getContainerId(),
          SchedulerUtils.LOST_CONTAINER),
        RMContainerEventType.KILL);
    }

    //Remove the node
    this.nodes.remove(rmNode.getNodeID());
    updateMaximumAllocation(node, false);

    // Update cluster metrics
    Resources.subtractFrom(clusterResource, node.getRMNode().getTotalCapability());
  }

  protected void increaseUsedResources(RMContainer rmContainer) {
    Resources.addTo(usedResource, rmContainer.getAllocatedResource());
  }

  protected void updateAppHeadRoom(SchedulerApplicationAttempt schedulerAttempt) {
    schedulerAttempt.setHeadroom(Resources.subtract(clusterResource, usedResource));
  }

  protected void printQueue() {
    if (logger.isDebugEnabled())
      logger.trace("Apps are now " + orderedApps);
  }

  protected abstract void updateAppPriority(A app);

  protected synchronized void onAppAttemptAdded(A app) {
    orderedApps.remove(app);
    updateAppPriority(app);
    orderedApps.add(app);
    logger.trace("Added attempt " + app.getApplicationAttemptId());
  }

  protected synchronized void onAppAttemptDone(A app) {
    Resources.subtractFrom(usedAMResource, app.getAMResource());
    orderedApps.remove(app);
    logger.trace("Removed attempt " + app.getApplicationAttemptId());
  }

  @Override
  protected PluginPolicyState exportState() {
    return new PluginPolicyState<>(clusterResource, nodes, applications);
  }

  @Override
  protected <T extends SchedulerNode & PluginSchedulerNode> void importState(PluginPolicyState<T> state) {
    clusterResource = state.getClusterResource();
    usedResource = Resource.newInstance(0, 0);
    usedAMResource = Resource.newInstance(0, 0);
    nodes = new ConcurrentHashMap<>();
    for (T node : state.getNodes().values()) {
      this.nodes.put(node.getNodeID(), FiCaPluginSchedulerNode.getInstance(nClass, node));
      updateMaximumAllocation(node, true);
      Resources.addTo(usedResource, node.getUsedResource());
    }
    queue.setAvailableResourcesToQueue(Resources.subtract(clusterResource, usedResource));
    for (Map.Entry<ApplicationId, ? extends SchedulerApplication<? extends SchedulerApplicationAttempt>> appEntry :
      state.getApplications().entrySet()) {
      SchedulerApplication<? extends SchedulerApplicationAttempt> app = appEntry.getValue();
      SchedulerApplication<A> newApp = new SchedulerApplication<>(app.getQueue(), app.getUser());
      applications.put(appEntry.getKey(), newApp);
      queue.getMetrics().submitApp(app.getUser());
      SchedulerApplicationAttempt attempt = app.getCurrentAppAttempt();
      if (attempt != null) {
        Resources.addTo(usedAMResource, Resources.min(getResourceCalculator(), clusterResource,
          attempt.getCurrentConsumption(), attempt.getAMResource()));
        A newAttempt = FiCaPluginApplicationAttempt.getInstance(aClass, attempt, queue.getActiveUsersManager(), rmContext);
        if (newAttempt instanceof Configurable)
          ((Configurable) newAttempt).setConf(conf);
        newApp.setCurrentAppAttempt(newAttempt);
        newAttempt.setHeadroomProvider(null);
        newAttempt.move(queue);
        onAppAttemptAdded(newApp.getCurrentAppAttempt());
      }
    }
    printQueue();
  }

  @Override
  public boolean forceContainerAssignment(ApplicationId appId, String hostName, Priority priority) {
    N node = null;
    for (Map.Entry<NodeId, N> nodeEntry : nodes.entrySet()) {
      if (nodeEntry.getKey().getHost().equals(hostName))
        node = nodeEntry.getValue();
    }
    if (node == null)
      throw new PosumException("Node could not be found for " + hostName);
    int containers = assignContainer(node, applications.get(appId).getCurrentAppAttempt(), priority, 1,
      ClusterUtils.createResourceRequest(minimumAllocation, hostName, 1), NodeType.NODE_LOCAL);
    return containers == 1;
  }

  @Override
  public Map<String, SchedulerNodeReport> getNodeReports() {
    Map<String, SchedulerNodeReport> reports = new HashMap<>(getNumClusterNodes());
    for (NodeId nodeId : nodes.keySet()) {
      reports.put(nodeId.getHost(), getNodeReport(nodeId));
    }
    return reports;
  }
}

