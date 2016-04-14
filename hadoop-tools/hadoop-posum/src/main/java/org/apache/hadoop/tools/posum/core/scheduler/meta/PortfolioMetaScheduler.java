package org.apache.hadoop.tools.posum.core.scheduler.meta;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.tools.posum.common.util.POSUMConfiguration;
import org.apache.hadoop.tools.posum.common.util.POSUMException;
import org.apache.hadoop.tools.posum.core.scheduler.portfolio.DataOrientedPolicy;
import org.apache.hadoop.tools.posum.core.scheduler.portfolio.FifoPolicy;
import org.apache.hadoop.tools.posum.core.scheduler.portfolio.PluginPolicy;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.proto.YarnServiceProtos;
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.*;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.QueueEntitlement;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.*;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by ane on 2/4/16.
 */
public class PortfolioMetaScheduler extends
        AbstractYarnScheduler<SchedulerApplicationAttempt, SchedulerNode> implements
        Configurable {

    private static Log logger = LogFactory.getLog(PortfolioMetaScheduler.class);

    private Configuration conf;
    private Configuration posumConf;
    private MetaSchedulerCommService commService;
    private Map<String, Class<? extends PluginPolicy>> policies;

    private Class<? extends PluginPolicy> currentPolicyClass = DefaultPolicy.FIFO.implClass;
    private PluginPolicy<? extends SchedulerApplicationAttempt, ? extends SchedulerNode, ?> currentPolicy;
    private ReadWriteLock lock = new ReentrantReadWriteLock();
    private Lock readLock = lock.readLock();
    private Lock writeLock = lock.writeLock();

    private enum DefaultPolicy {
        FIFO(FifoPolicy.class),
        DATA(DataOrientedPolicy.class);

        Class<? extends PluginPolicy> implClass;

        DefaultPolicy(Class<? extends PluginPolicy> implClass) {
            this.implClass = implClass;
        }
    }

    public PortfolioMetaScheduler() {
        super(PortfolioMetaScheduler.class.getName());
    }

    private void preparePolicies() {
        String policyMap = posumConf.get(POSUMConfiguration.SCHEDULER_POLICY_MAP);
        policies = new HashMap<>(DefaultPolicy.values().length);
        if (policyMap != null) {
            try {
                for (String entry : policyMap.split(",")) {
                    String[] entryParts = entry.split("=");
                    if (entryParts.length != 2)
                        policies.put(entryParts[0],
                                (Class<? extends PluginPolicy>) getClass().getClassLoader().loadClass(entryParts[1]));
                }
            } catch (Exception e) {
                throw new POSUMException("Could not parse policy map");
            }
        } else {
            for (DefaultPolicy policy : DefaultPolicy.values()) {
                policies.put(policy.name(), policy.implClass);
            }
        }
    }

    private void initPolicy() {
        try {
            currentPolicy = currentPolicyClass.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new POSUMException("Could not instantiate scheduler for class " + currentPolicyClass, e);
        }
        currentPolicy.initializePlugin(posumConf);
        if (rmContext != null)
            currentPolicy.setRMContext(rmContext);
        logger.debug("Initializing current policy");
        currentPolicy.init(conf);
    }

    private void transferState(PluginPolicy oldPolicy) {
        //TODO transfer everything
        if (isInState(STATE.STARTED)) {
            logger.debug("Starting current policy");
            currentPolicy.start();
        }
    }

    protected void changeToPolicy(String policyName) {
        logger.debug("Changing policy to " + policyName);
        Class<? extends PluginPolicy> newClass = policies.get(policyName);
        if (newClass == null)
            throw new POSUMException("Target policy does not exist: " + policyName);
        if (!currentPolicyClass.equals(newClass)) {
            writeLock.lock();
            currentPolicyClass = newClass;
            if (isInState(STATE.INITED) || isInState(STATE.STARTED)) {
                PluginPolicy oldPolicy = currentPolicy;
                initPolicy();
                if (oldPolicy != null)
                    transferState(oldPolicy);
            }
            writeLock.unlock();
            logger.debug("Policy changed successfully");
        }

    }

    private void initComm() {
        commService = new MetaSchedulerCommService(this);
        commService.init(posumConf);
        commService.start();
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

    @Override
    public void serviceInit(Configuration conf) throws Exception {
        logger.debug("Service init called for meta");
        this.posumConf = POSUMConfiguration.newInstance();
        setConf(conf);
        preparePolicies();
        initComm();
        initPolicy();
    }

    @Override
    public void serviceStart() throws Exception {
        logger.debug("Starting meta");

        readLock.lock();
        try {
            currentPolicy.start();
        } finally {
            readLock.unlock();
        }
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
    }

    @Override
    public int getNumClusterNodes() {
        readLock.lock();
        try {
            return currentPolicy.getNumClusterNodes();
        } finally {
            readLock.unlock();
        }
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
        readLock.lock();
        try {
            return currentPolicy.allocate(applicationAttemptId, ask,
                    release, blacklistAdditions, blacklistRemovals);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void handle(SchedulerEvent event) {
        readLock.lock();
        try {
            currentPolicy.handle(event);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public QueueInfo getQueueInfo(String queueName,
                                  boolean includeChildQueues, boolean recursive) throws IOException {
        readLock.lock();
        try {
            return currentPolicy.getQueueInfo(queueName, includeChildQueues, recursive);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public List<QueueUserACLInfo> getQueueUserAclInfo() {
        readLock.lock();
        try {
            return currentPolicy.getQueueUserAclInfo();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public ResourceCalculator getResourceCalculator() {
        readLock.lock();
        try {
            return currentPolicy.getResourceCalculator();
        } finally {
            readLock.unlock();
        }
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
        readLock.lock();
        try {
            return currentPolicy.getRMContainer(containerId);
        } finally {
            readLock.unlock();
        }
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
        readLock.lock();
        try {
            return currentPolicy.getRootQueueMetrics();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public synchronized boolean checkAccess(UserGroupInformation callerUGI,
                                            QueueACL acl, String queueName) {
        readLock.lock();
        try {
            return currentPolicy.checkAccess(callerUGI, acl, queueName);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public synchronized List<ApplicationAttemptId> getAppsInQueue(String queueName) {
        readLock.lock();
        try {
            return currentPolicy.getAppsInQueue(queueName);
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Methods that the MetaScheduler must override because it is a dummy scheduler
     */

    @Override
    public synchronized List<Container> getTransferredContainers(ApplicationAttemptId currentAttempt) {
        readLock.lock();
        try {
            return currentPolicy.getTransferredContainers(currentAttempt);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public Map<ApplicationId, SchedulerApplication<SchedulerApplicationAttempt>> getSchedulerApplications() {
        readLock.lock();
        try {
            // explicit conversion required due to currentPolicy outputting SchedulerApplication<? extends SchedulerApplicationAttempt>>
            Map<ApplicationId, ? extends SchedulerApplication<? extends SchedulerApplicationAttempt>> apps =
                    currentPolicy.getSchedulerApplications();
            Map<ApplicationId, SchedulerApplication<SchedulerApplicationAttempt>> ret = new HashMap<>(apps.size());

            for (Map.Entry<ApplicationId, ? extends SchedulerApplication<? extends SchedulerApplicationAttempt>> entry :
                    apps.entrySet()) {
                ret.put(entry.getKey(), (SchedulerApplication<SchedulerApplicationAttempt>) entry.getValue());
            }
            return ret;
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public Resource getClusterResource() {
        readLock.lock();
        try {
            return currentPolicy.getClusterResource();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public Resource getMinimumResourceCapability() {
        readLock.lock();
        try {
            return currentPolicy.getMinimumResourceCapability();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public Resource getMaximumResourceCapability() {
        readLock.lock();
        try {
            return currentPolicy.getMaximumResourceCapability();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public Resource getMaximumResourceCapability(String queueName) {
        readLock.lock();
        try {
            return currentPolicy.getMaximumResourceCapability(queueName);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public SchedulerApplicationAttempt getApplicationAttempt(ApplicationAttemptId applicationAttemptId) {
        readLock.lock();
        try {
            return currentPolicy.getApplicationAttempt(applicationAttemptId);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public SchedulerAppReport getSchedulerAppInfo(ApplicationAttemptId appAttemptId) {
        readLock.lock();
        try {
            return currentPolicy.getSchedulerAppInfo(appAttemptId);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public ApplicationResourceUsageReport getAppResourceUsageReport(ApplicationAttemptId appAttemptId) {
        readLock.lock();
        try {
            return currentPolicy.getAppResourceUsageReport(appAttemptId);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public SchedulerApplicationAttempt getCurrentAttemptForContainer(ContainerId containerId) {
        readLock.lock();
        try {
            return currentPolicy.getCurrentAttemptForContainer(containerId);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public SchedulerNodeReport getNodeReport(NodeId nodeId) {
        readLock.lock();
        try {
            return currentPolicy.getNodeReport(nodeId);
        } finally {
            readLock.unlock();
        }
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
        readLock.lock();
        try {
            currentPolicy.moveAllApps(sourceQueue, destQueue);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public synchronized void killAllAppsInQueue(String queueName) throws YarnException {
        readLock.lock();
        try {
            currentPolicy.killAllAppsInQueue(queueName);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public synchronized void updateNodeResource(RMNode nm, ResourceOption resourceOption) {
        readLock.lock();
        try {
            currentPolicy.updateNodeResource(nm, resourceOption);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public EnumSet<YarnServiceProtos.SchedulerResourceTypes> getSchedulingResourceTypes() {
        readLock.lock();
        try {
            return currentPolicy.getSchedulingResourceTypes();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public Set<String> getPlanQueues() throws YarnException {
        readLock.lock();
        try {
            return currentPolicy.getPlanQueues();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public List<ResourceRequest> getPendingResourceRequestsForAttempt(ApplicationAttemptId attemptId) {
        readLock.lock();
        try {
            return currentPolicy.getPendingResourceRequestsForAttempt(attemptId);
        } finally {
            readLock.unlock();
        }
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
}
