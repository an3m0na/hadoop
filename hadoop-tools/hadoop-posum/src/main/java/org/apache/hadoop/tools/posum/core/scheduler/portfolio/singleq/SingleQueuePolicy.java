package org.apache.hadoop.tools.posum.core.scheduler.portfolio.singleq;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.tools.posum.common.util.PosumException;
import org.apache.hadoop.tools.posum.core.scheduler.portfolio.PluginPolicy;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.api.records.Priority;
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
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.*;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.*;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * Created by ane on 1/22/16.
 */
public abstract class SingleQueuePolicy<A extends SQSAppAttempt,
        N extends SQSchedulerNode,
        Q extends SQSQueue,
        S extends SingleQueuePolicy<A, N, Q, S>>
        extends PluginPolicy<A, N> {

    private static Log LOG = LogFactory.getLog(SingleQueuePolicy.class);

    Configuration conf;
    private static final String DEFAULT_QUEUE_NAME = "default";
    private static final RecordFactory recordFactory =
            RecordFactoryProvider.getRecordFactory(null);

    protected Resource usedResource = recordFactory.newRecordInstance(Resource.class);
    private boolean usePortForNodeName;
    private final ResourceCalculator resourceCalculator = new DefaultResourceCalculator();

    protected Q queue;
    private Class<Q> qClass;
    protected ConcurrentSkipListSet<SchedulerApplication<A>> orderedApps;

    public SingleQueuePolicy(Class<A> aClass, Class<N> nClass, Class<Q> qClass, Class<S> sClass) {
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

    protected abstract Comparator<SchedulerApplication<A>> initQueueComparator();

    protected synchronized void initScheduler(Configuration conf) {
        validateConf(conf);
        LOG.debug("Configuration valid");

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
        this.orderedApps = new ConcurrentSkipListSet<>(initQueueComparator());
        this.queue = SQSQueue.getInstance(qClass, DEFAULT_QUEUE_NAME, this);

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
            LOG.info("Null container completed...");
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
            LOG.trace("Unknown application: " + appId +
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

        LOG.trace("Application attempt " + application.getApplicationAttemptId() +
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
            LOG.error("Calling allocate on removed " +
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
                LOG.info("Calling allocate on a stopped " +
                        "application " + applicationAttemptId);
                return EMPTY_ALLOCATION;
            }

            if (!ask.isEmpty()) {
                LOG.trace("allocate: pre-update" +
                        " applicationId=" + applicationAttemptId +
                        " application=" + application);
                application.showRequests();

                // Update application requests
                application.updateResourceRequests(ask);

                LOG.trace("allocate: post-update" +
                        " applicationId=" + applicationAttemptId +
                        " application=" + application);
                application.showRequests();

                LOG.trace("allocate:" +
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
                    LOG.error("Unable to remove application "
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
                LOG.error("Unknown event arrived at scheduler:" + event.toString());
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
        // Kill all 'live' containers
        for (RMContainer container : attempt.getLiveContainers()) {
            if (keepContainers
                    && container.getState().equals(RMContainerState.RUNNING)) {
                // do not kill the running container in the case of work-preserving AM
                // restart.
                LOG.info("Skip killing " + container.getContainerId());
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
        // TODO: Fix store
        A schedulerAppAttempt = SQSAppAttempt.getInstance(aClass, appAttemptId, user, queue,
                new ActiveUsersManager(queue.getMetrics()), this.rmContext);

        if (transferStateFromPreviousAttempt) {
            schedulerAppAttempt.transferStateFromPreviousAttempt(application
                    .getCurrentAppAttempt());
        }

        application.setCurrentAppAttempt(schedulerAppAttempt);

        onAppAttemptAdded(application);

        queue.getMetrics().submitAppAttempt(user);
        LOG.info("Added Application Attempt " + appAttemptId
                + " to scheduler from user " + application.getUser());
        if (isAttemptRecovering) {
            if (LOG.isDebugEnabled()) {
                LOG.trace(appAttemptId
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
            LOG.warn("Couldn't find application " + applicationId);
            return;
        }

        // Inform the activeUsersManager
        queue.deactivateApplication(application.getUser(),
                applicationId);
        application.stop(finalState);
        applications.remove(applicationId);
        onAppDone(application);
    }

    private void addApplication(ApplicationId applicationId, String user, boolean isAppRecovering) {
        SchedulerApplication<A> application =
                new SchedulerApplication<>(queue, user);
        applications.put(applicationId, application);
        onAppAdded(application);

        queue.getMetrics().submitApp(user);
        LOG.info("Accepted application " + applicationId + " from user: " + user
                + ", currently num of applications: " + applications.size());
        if (isAppRecovering) {
            if (LOG.isDebugEnabled()) {
                LOG.trace(applicationId + " is recovering. Skip notifying APP_ACCEPTED");
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
            LOG.trace("Container FINISHED: " + containerId);
            completedContainer(getRMContainer(containerId),
                    completedContainer, RMContainerEventType.FINISHED);
        }

        if (Resources.greaterThanOrEqual(resourceCalculator, clusterResource,
                node.getAvailableResource(), minimumAllocation)) {
            LOG.trace("Node heartbeat " + rmNode.getNodeID() +
                    " available resource = " + node.getAvailableResource());

            assignContainers(node);

            LOG.trace("Node after allocation " + rmNode.getNodeID() + " resource = "
                    + node.getAvailableResource());
        }

        queue.setAvailableResourcesToQueue(Resources.subtract(clusterResource,
                usedResource));
    }

    protected boolean assignToApp(N node, SchedulerApplication<A> app) {
        A application = app.getCurrentAppAttempt();
        if (application == null) {
            return false;
        }

        LOG.trace("pre-assignContainers");
        application.showRequests();
        synchronized (application) {
            // Check if this resource is on the blacklist
            if (SchedulerAppUtils.isBlacklisted(application, node, LOG)) {
                return false;
            }
            for (Priority priority : application.getPriorities()) {
                int maxContainers =
                        getMaxAllocatableContainers(application, priority, node,
                                NodeType.OFF_SWITCH);
                // Ensure the application needs containers of this priority
                if (maxContainers > 0) {
                    int assignedContainers =
                            assignContainersOnNode(node, application, priority);
                    // Do not assign out of order w.r.t priorities
                    if (assignedContainers == 0) {
                        break;
                    }
                }
            }
        }

        LOG.trace("post-assignContainers");
        application.showRequests();

        // Done
        if (Resources.lessThan(resourceCalculator, clusterResource,
                node.getAvailableResource(), minimumAllocation)) {
            return true;
        }
        return false;
    }

    protected abstract void assignFromQueue(N node);

    /**
     * Heart of the scheduler...
     *
     * @param node node on which resources are available to be allocated
     */
    private void assignContainers(N node) {

        LOG.trace("assignContainers:" +
                " node=" + node.getRMNode().getNodeAddress() +
                " #applications=" + applications.size());

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

    private int getMaxAllocatableContainers(A application,
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


    private int assignContainersOnNode(N node,
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


        LOG.debug("assignContainersOnNode:" +
                " node=" + node.getRMNode().getNodeAddress() +
                " application=" + application.getApplicationId().getId() +
                " priority=" + priority.getPriority() +
                " #assigned=" +
                (nodeLocalContainers + rackLocalContainers + offSwitchContainers));


        return (nodeLocalContainers + rackLocalContainers + offSwitchContainers);
    }

    private int assignNodeLocalContainers(N node,
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

    private int assignRackLocalContainers(N node,
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

    private int assignOffSwitchContainers(N node,
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
        LOG.trace("assignContainers:" +
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
        N schedulerNode = SQSchedulerNode.getInstance(nClass, rmNode, usePortForNodeName);
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
        schedulerAttempt.setHeadroom(Resources.subtract(clusterResource,
                usedResource));
    }

    protected void printQueue() {
        StringBuilder builder = new StringBuilder("Apps are now [ ");
        for (SchedulerApplication<A> orderedApp : orderedApps) {
            A attempt = orderedApp.getCurrentAppAttempt();
            if (attempt != null)
                builder.append(attempt.toString());
            else
                builder.append("unknown");
            builder.append(" ");
        }
        builder.append("]");
        LOG.debug(builder.toString());
    }

    protected abstract void updateAppPriority(SchedulerApplication<A> app);

    protected void onAppAttemptAdded(SchedulerApplication<A> app) {
        orderedApps.remove(app);
        updateAppPriority(app);
        orderedApps.add(app);
        printQueue();
    }

    protected void onAppAdded(SchedulerApplication<A> app) {
        orderedApps.add(app);
        printQueue();
    }

    protected void onAppDone(SchedulerApplication<A> app) {
        orderedApps.remove(app);
        printQueue();
    }


    public void assumeState(PluginPolicyState state) {
        this.usedResource = state.usedResource;
        this.clusterResource = state.clusterResource;
        this.usePortForNodeName = state.usePortForNodeName;
        this.nodes = new ConcurrentHashMap<>();
        for (SQSchedulerNode node : state.nodes.values()) {
            this.nodes.put(node.getNodeID(), SQSchedulerNode.getInstance(nClass, node));
            updateMaximumAllocation(node, true);
        }
        queue.setAvailableResourcesToQueue(Resources.subtract(clusterResource,
                usedResource));
        for (Map.Entry<ApplicationId, ? extends SchedulerApplication<? extends SQSAppAttempt>> appEntry :
                state.applications.entrySet()) {
            SchedulerApplication<? extends SQSAppAttempt> app = appEntry.getValue();
            SchedulerApplication<A> newApp = new SchedulerApplication<>(app.getQueue(), app.getUser());
            this.applications.put(appEntry.getKey(), newApp);
            queue.getMetrics().submitApp(app.getUser());
            onAppAdded(newApp);
            SQSAppAttempt attempt = app.getCurrentAppAttempt();
            if (attempt != null) {
                newApp.setCurrentAppAttempt(SQSAppAttempt.getInstance(aClass, attempt));
                queue.getMetrics().submitAppAttempt(app.getUser());
                onAppAttemptAdded(newApp);
            }
        }
        printQueue();
    }

    public PluginPolicyState exportState() {
        return new PluginPolicyState(this.usedResource, this.queue, this.nodes, this.applications, this.clusterResource, getMaximumResourceCapability(), this.usePortForNodeName);
    }

    public void transferStateFromPolicy(SingleQueuePolicy other) {
        assumeState(other.exportState());
    }

    @Override
    public void transferStateFromPolicy(PluginPolicy other) {
        if (PluginPolicy.class.isAssignableFrom(SingleQueuePolicy.class)) {
            transferStateFromPolicy((SingleQueuePolicy) other);
            return;
        }
        //TODO
        throw new PosumException("Cannot transfer state from unknown policy " + other.getClass().getName());
    }
}

