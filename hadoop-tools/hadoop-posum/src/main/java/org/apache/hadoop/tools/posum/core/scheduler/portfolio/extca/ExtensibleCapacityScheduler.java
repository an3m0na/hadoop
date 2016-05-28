package org.apache.hadoop.tools.posum.core.scheduler.portfolio.extca;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.tools.posum.common.util.POSUMException;
import org.apache.hadoop.tools.posum.common.util.Utils;
import org.apache.hadoop.tools.posum.core.scheduler.portfolio.PluginPolicy;
import org.apache.hadoop.yarn.api.records.*;
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
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.*;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.*;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.QueueEntitlement;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.*;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.utils.Lock;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by ane on 1/22/16.
 */
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

    protected void writeField(String name, Object value) {
        try {
            Field field = Utils.findField(CapacityScheduler.class, name);
            field.setAccessible(true);
            field.set(inner, value);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new POSUMException("Reflection error: ", e);
        }
    }

    protected <T> T readField(String name) {
        try {
            Field field = Utils.findField(CapacityScheduler.class, name);
            field.setAccessible(true);
            return (T) field.get(inner);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new POSUMException("Reflection error: ", e);
        }
    }


    protected <T> T invokeMethod(String name, Class<?>[] paramTypes, Object... args) {
        try {
            Method method = Utils.findMethod(CapacityScheduler.class, name, paramTypes);
            method.setAccessible(true);
            return (T) method.invoke(inner, args);
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            throw new POSUMException("Reflection error: ", e);
        }
    }

    //
    // <------ Methods to be overridden when extending scheduler ------>
    //

    protected CapacitySchedulerConfiguration loadCustomCapacityConf(Configuration conf) {
        return new CapacitySchedulerConfiguration(conf);
    }

    protected void updateAppPriority(A app) {

    }

    protected void onAppAttemptAdded(A app) {
        updateAppPriority(app);
    }

    protected String resolveQueue(String queue,
                                  ApplicationId applicationId,
                                  boolean isAppRecovering,
                                  ReservationId reservationID) {
        return queue;
    }

    protected boolean checkIfPrioritiesExpired() {
        return false;
    }

    protected void updateApplicationPriorities(LeafQueue queue, String applicationSetName) {
        try {
            Field appsField = LeafQueue.class.getField(applicationSetName);
            appsField.setAccessible(true);
            Set<FiCaSchedulerApp> apps = (Set<FiCaSchedulerApp>) appsField.get(queue);
            for (Iterator<FiCaSchedulerApp> i = apps.iterator(); i.hasNext(); ) {
                A app = (A) i.next();
                // remove, update and add to resort
                i.remove();
                updateAppPriority(app);
                apps.add(app);
            }

        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new POSUMException("Reflection error: ", e);
        }

    }

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
                    ExtCaAppAttempt.getInstance(aClass, applicationAttemptId, application.getUser(),
                            queue, queue.getActiveUsersManager(), inner.getRMContext());
            if (transferStateFromPreviousAttempt) {
                attempt.transferStateFromPreviousAttempt(application
                        .getCurrentAppAttempt());
            }
            application.setCurrentAppAttempt(attempt);

            onAppAttemptAdded(attempt);

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

            // update this node to node label manager
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
        invokeMethod("allocateContainersToNode", new Class<?>[]{FiCaSchedulerNode.class}, node);
    }

    protected void initializeWithModifiedConf() {
        synchronized (inner) {
            writeField("conf", capacityConf);
            invokeMethod("validateConf", new Class<?>[]{Configuration.class}, capacityConf);
            writeField("calculator", capacityConf.getResourceCalculator());
            invokeMethod("reinitializeQueues", new Class<?>[]{CapacitySchedulerConfiguration.class}, capacityConf);
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
                Map<NodeId, FiCaSchedulerNode> nodes = readField("nodes");
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
        Map<ApplicationId, SchedulerApplication<FiCaSchedulerApp>> apps =
                inner.getSchedulerApplications();
        Map<ApplicationId, SchedulerApplication<A>> ret = new HashMap<>(apps.size());

        for (Map.Entry<ApplicationId, SchedulerApplication<FiCaSchedulerApp>> entry :
                apps.entrySet()) {
            SchedulerApplication<A> app = new SchedulerApplication<>(entry.getValue().getQueue(), entry.getValue().getUser());
            app.setCurrentAppAttempt((A) entry.getValue().getCurrentAppAttempt());
            ret.put(entry.getKey(), app);
        }
        return ret;
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
    public Comparator<FiCaSchedulerApp> getApplicationComparator() {
        return inner.getApplicationComparator();
    }

    @Override
    public ResourceCalculator getResourceCalculator() {
        return inner.getResourceCalculator();
    }

    @Override
    public Comparator<CSQueue> getQueueComparator() {
        return inner.getQueueComparator();
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

    @Override
    public void assumeState(PluginPolicyState state) {
        //TODO
//        this.usedResource = state.usedResource;
//        this.clusterResource = state.clusterResource;
//        this.usePortForNodeName = state.usePortForNodeName;
//        this.nodes = new ConcurrentHashMap<>();
//        for (SQSchedulerNode node : state.nodes.values()) {
//            this.nodes.put(node.getNodeID(), SQSchedulerNode.getInstance(nClass, node));
//            updateMaximumAllocation(node, true);
//        }
//        queue.setAvailableResourcesToQueue(Resources.subtract(clusterResource,
//                usedResource));
//        for (Map.Entry<ApplicationId, ? extends SchedulerApplication<? extends SQSAppAttempt>> appEntry :
//                state.applications.entrySet()) {
//            SchedulerApplication<? extends SQSAppAttempt> app = appEntry.getValue();
//            SchedulerApplication<A> newApp = new SchedulerApplication<>(app.getQueue(), app.getUser());
//            this.applications.put(appEntry.getKey(), newApp);
//            queue.getMetrics().submitApp(app.getUser());
//            onAppAdded(newApp);
//            SQSAppAttempt attempt = app.getCurrentAppAttempt();
//            if (attempt != null) {
//                newApp.setCurrentAppAttempt(SQSAppAttempt.getInstance(aClass, attempt));
//                queue.getMetrics().submitAppAttempt(app.getUser());
//                onAppAttemptAdded(newApp);
//            }
//        }
//        printQueue();
    }


    @Override
    public PluginPolicyState exportState() {
        //TODO
        return null;
//        return new PluginPolicyState(this.usedResource, this.queue, this.nodes, this.applications, this.clusterResource, getMaximumResourceCapability(), this.usePortForNodeName);
    }
}

