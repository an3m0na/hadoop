package org.apache.hadoop.tools.posum.master.scheduler.data;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.*;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by ane on 1/22/16.
 */
public class DataOrientedScheduler extends AbstractYarnScheduler<DOSAppAttempt, DOSchedulerNode> implements
        Configurable {

    private static Log logger = LogFactory.getLog(DataOrientedScheduler.class);

    Configuration conf;
    private static final String DEFAULT_QUEUE_NAME = "default";
    private static final RecordFactory recordFactory =
            RecordFactoryProvider.getRecordFactory(null);

    private Resource usedResource = recordFactory.newRecordInstance(Resource.class);
    private boolean usePortForNodeName;
    private final ResourceCalculator resourceCalculator = new DefaultResourceCalculator();

    private DOSQueue queue;

    public DataOrientedScheduler() {
        super(DataOrientedScheduler.class.getName());
    }

    public Resource getUsedResource() {
        return usedResource;
    }

    private void validateConf(Configuration conf) {
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

    private synchronized void initScheduler(Configuration conf) {
        validateConf(conf);

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
        System.out.println(maxResource);
        initMaximumResourceCapability(maxResource);
        this.usePortForNodeName = conf.getBoolean(
                YarnConfiguration.RM_SCHEDULER_INCLUDE_PORT_IN_NODE_NAME,
                YarnConfiguration.DEFAULT_RM_SCHEDULER_USE_PORT_FOR_NODE_NAME);
        this.applications = new ConcurrentHashMap<>();
        this.queue = new DOSQueue(DEFAULT_QUEUE_NAME, this);
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
            logger.info("Null container completed...");
            return;
        }

        // Get the application for the finished container
        Container container = rmContainer.getContainer();
        DOSAppAttempt application =
                getCurrentAttemptForContainer(container.getId());
        ApplicationId appId =
                container.getId().getApplicationAttemptId().getApplicationId();

        // Get the node on which the container was allocated
        DOSchedulerNode node = nodes.get(container.getNodeId());

        if (application == null) {
            logger.info("Unknown application: " + appId +
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

        logger.info("Application attempt " + application.getApplicationAttemptId() +
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
            List<ContainerId> release, List<String> blacklistAdditions, List<String> blacklistRemovals){

        //Same everywhere

    DOSAppAttempt application = getApplicationAttempt(applicationAttemptId);
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
                logger.debug("allocate: pre-update" +
                        " applicationId=" + applicationAttemptId +
                        " application=" + application);
                application.showRequests();

                // Update application requests
                application.updateResourceRequests(ask);

                logger.debug("allocate: post-update" +
                        " applicationId=" + applicationAttemptId +
                        " application=" + application);
                application.showRequests();

                logger.debug("allocate:" +
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
            for (SchedulerApplication<DOSAppAttempt> app : applications.values()) {
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
            case NODE_ADDED: {
                NodeAddedSchedulerEvent nodeAddedEvent = (NodeAddedSchedulerEvent) event;
                addNode(nodeAddedEvent.getAddedRMNode());
                recoverContainersOnNode(nodeAddedEvent.getContainerReports(),
                        nodeAddedEvent.getAddedRMNode());

            }
            break;
            case NODE_REMOVED: {
                NodeRemovedSchedulerEvent nodeRemovedEvent = (NodeRemovedSchedulerEvent) event;
                removeNode(nodeRemovedEvent.getRemovedRMNode());
            }
            break;
            case NODE_RESOURCE_UPDATE: {
                NodeResourceUpdateSchedulerEvent nodeResourceUpdatedEvent =
                        (NodeResourceUpdateSchedulerEvent) event;
                updateNodeResource(nodeResourceUpdatedEvent.getRMNode(),
                        nodeResourceUpdatedEvent.getResourceOption());
            }
            break;
            case NODE_UPDATE: {
                NodeUpdateSchedulerEvent nodeUpdatedEvent =
                        (NodeUpdateSchedulerEvent) event;
                nodeUpdate(nodeUpdatedEvent.getRMNode());
            }
            break;
            case APP_ADDED: {
                AppAddedSchedulerEvent appAddedEvent = (AppAddedSchedulerEvent) event;
                addApplication(appAddedEvent.getApplicationId(), appAddedEvent.getUser(),
                        appAddedEvent.getIsAppRecovering());
            }
            break;
            case APP_REMOVED: {
                AppRemovedSchedulerEvent appRemovedEvent = (AppRemovedSchedulerEvent) event;
                doneApplication(appRemovedEvent.getApplicationID(),
                        appRemovedEvent.getFinalState());
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
            case APP_ATTEMPT_REMOVED: {
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
            }
            break;
            case CONTAINER_EXPIRED: {
                ContainerExpiredSchedulerEvent containerExpiredEvent =
                        (ContainerExpiredSchedulerEvent) event;
                ContainerId containerid = containerExpiredEvent.getContainerId();
                completedContainer(getRMContainer(containerid),
                        SchedulerUtils.createAbnormalContainerStatus(
                                containerid,
                                SchedulerUtils.EXPIRED_CONTAINER),
                        RMContainerEventType.EXPIRE);
            }
            break;
            default:
                logger.error("Invalid eventtype " + event.getType() + ". Ignoring!");
        }
    }

    private void doneApplicationAttempt(ApplicationAttemptId appAttemptId,
                                        RMAppAttemptState finalAttemptState,
                                        boolean keepContainers)
            throws IOException {

        //Just like FIFO

        DOSAppAttempt attempt = getApplicationAttempt(appAttemptId);
        SchedulerApplication<DOSAppAttempt> application =
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
        SchedulerApplication<DOSAppAttempt> application =
                applications.get(appAttemptId.getApplicationId());
        String user = application.getUser();
        // TODO: Fix store
        DOSAppAttempt schedulerAppAttempt =
                new DOSAppAttempt(appAttemptId, user, queue,
                        new ActiveUsersManager(queue.getMetrics()), this.rmContext);

        if (transferStateFromPreviousAttempt) {
            schedulerAppAttempt.transferStateFromPreviousAttempt(application
                    .getCurrentAppAttempt());
        }
        application.setCurrentAppAttempt(schedulerAppAttempt);

        queue.getMetrics().submitAppAttempt(user);
        logger.info("Added Application Attempt " + appAttemptId
                + " to scheduler from user " + application.getUser());
        if (isAttemptRecovering) {
            if (logger.isDebugEnabled()) {
                logger.debug(appAttemptId
                        + " is recovering. Skipping notifying ATTEMPT_ADDED");
            }
        } else {
            rmContext.getDispatcher().getEventHandler().handle(
                    new RMAppAttemptEvent(appAttemptId,
                            RMAppAttemptEventType.ATTEMPT_ADDED));
        }
    }

    private void doneApplication(ApplicationId applicationId, RMAppState finalState) {
        SchedulerApplication<DOSAppAttempt> application =
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
        SchedulerApplication<DOSAppAttempt> application =
                new SchedulerApplication<>(queue, user);
        applications.put(applicationId, application);
        queue.getMetrics().submitApp(user);
        logger.info("Accepted application " + applicationId + " from user: " + user
                + ", currently num of applications: " + applications.size());
        if (isAppRecovering) {
            if (logger.isDebugEnabled()) {
                logger.debug(applicationId + " is recovering. Skip notifying APP_ACCEPTED");
            }
        } else {
            rmContext.getDispatcher().getEventHandler()
                    .handle(new RMAppEvent(applicationId, RMAppEventType.APP_ACCEPTED));
        }
    }

    private void nodeUpdate(RMNode rmNode) {

        // Just like FIFO

        DOSchedulerNode node = nodes.get(rmNode.getNodeID());

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
            logger.debug("Container FINISHED: " + containerId);
            completedContainer(getRMContainer(containerId),
                    completedContainer, RMContainerEventType.FINISHED);
        }

        if (Resources.greaterThanOrEqual(resourceCalculator, clusterResource,
                node.getAvailableResource(), minimumAllocation)) {
            logger.debug("Node heartbeat " + rmNode.getNodeID() +
                    " available resource = " + node.getAvailableResource());

            assignContainers(node);

            logger.debug("Node after allocation " + rmNode.getNodeID() + " resource = "
                    + node.getAvailableResource());
        }

        queue.setAvailableResourcesToQueue(Resources.subtract(clusterResource,
                usedResource));
    }

    /**
     * Heart of the scheduler...
     *
     * @param node node on which resources are available to be allocated
     */
    private void assignContainers(DOSchedulerNode node) {
        //TODO magic

        logger.debug("assignContainers:" +
                " node=" + node.getRMNode().getNodeAddress() +
                " #applications=" + applications.size());

        // Try to assign containers to applications in fifo order
        for (Map.Entry<ApplicationId, SchedulerApplication<DOSAppAttempt>> e : applications
                .entrySet()) {
            DOSAppAttempt application = e.getValue().getCurrentAppAttempt();
            if (application == null) {
                continue;
            }

            logger.debug("pre-assignContainers");
            application.showRequests();
            synchronized (application) {
                // Check if this resource is on the blacklist
                if (SchedulerAppUtils.isBlacklisted(application, node, logger)) {
                    continue;
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

            logger.debug("post-assignContainers");
            application.showRequests();

            // Done
            if (Resources.lessThan(resourceCalculator, clusterResource,
                    node.getAvailableResource(), minimumAllocation)) {
                break;
            }
        }

        // Update the applications' headroom to correctly take into
        // account the containers assigned in this update.
        for (SchedulerApplication<DOSAppAttempt> application : applications.values()) {
            DOSAppAttempt attempt =
                    (DOSAppAttempt) application.getCurrentAppAttempt();
            if (attempt == null) {
                continue;
            }
            updateAppHeadRoom(attempt);
        }
    }

    private int getMaxAllocatableContainers(DOSAppAttempt application,
                                            Priority priority, DOSchedulerNode node, NodeType type) {
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


    private int assignContainersOnNode(DOSchedulerNode node,
                                       DOSAppAttempt application, Priority priority
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


        logger.debug("assignContainersOnNode:" +
                " node=" + node.getRMNode().getNodeAddress() +
                " application=" + application.getApplicationId().getId() +
                " priority=" + priority.getPriority() +
                " #assigned=" +
                (nodeLocalContainers + rackLocalContainers + offSwitchContainers));


        return (nodeLocalContainers + rackLocalContainers + offSwitchContainers);
    }

    private int assignNodeLocalContainers(DOSchedulerNode node,
                                          DOSAppAttempt application, Priority priority) {
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

    private int assignRackLocalContainers(DOSchedulerNode node,
                                          DOSAppAttempt application, Priority priority) {
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

    private int assignOffSwitchContainers(DOSchedulerNode node,
                                          DOSAppAttempt application, Priority priority) {
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

    private int assignContainer(DOSchedulerNode node, DOSAppAttempt application,
                                Priority priority, int assignableContainers,
                                ResourceRequest request, NodeType type) {
        logger.debug("assignContainers:" +
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
            for (int i=0; i < assignedContainers; ++i) {

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

    private void addNode(RMNode rmNode) {
        DOSchedulerNode schedulerNode = new DOSchedulerNode(rmNode,
                usePortForNodeName);
        this.nodes.put(rmNode.getNodeID(), schedulerNode);
        Resources.addTo(clusterResource, rmNode.getTotalCapability());
        updateMaximumAllocation(schedulerNode, true);
    }

    private synchronized void removeNode(RMNode rmNode) {

        // Generally the same for everybody

        DOSchedulerNode node = nodes.get(rmNode.getNodeID());
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

}

