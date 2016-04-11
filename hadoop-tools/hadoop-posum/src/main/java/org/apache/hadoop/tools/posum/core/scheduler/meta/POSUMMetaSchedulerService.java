package org.apache.hadoop.tools.posum.core.scheduler.meta;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.tools.posum.common.records.protocol.DataMasterProtocol;
import org.apache.hadoop.tools.posum.common.records.message.HandleRMEventRequest;
import org.apache.hadoop.tools.posum.common.records.protocol.MetaSchedulerProtocol;
import org.apache.hadoop.tools.posum.common.records.message.simple.SimpleResponse;
import org.apache.hadoop.tools.posum.common.util.DummyTokenSecretManager;
import org.apache.hadoop.tools.posum.common.util.POSUMConfiguration;
import org.apache.hadoop.tools.posum.core.scheduler.meta.client.PolicyPortfolioClient;
import org.apache.hadoop.tools.posum.core.scheduler.portfolio.singleq.SQSAppAttempt;
import org.apache.hadoop.tools.posum.core.scheduler.portfolio.singleq.SQSchedulerNode;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.proto.YarnServiceProtos;
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.*;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.QueueEntitlement;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.*;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by ane on 2/4/16.
 */
public class POSUMMetaSchedulerService extends
        AbstractYarnScheduler<SQSAppAttempt, SQSchedulerNode> implements
        Configurable, MetaSchedulerProtocol {

    private static Log logger = LogFactory.getLog(POSUMMetaSchedulerService.class);

    private Configuration conf;
    private Configuration posumConf;
    private PolicyPortfolioClient portfolioClient;
    private Server server;
    private InetSocketAddress bindAddress;


    public POSUMMetaSchedulerService() {
        super(POSUMMetaSchedulerService.class.getName());
    }


    private void initializePortfolioLink() {
        posumConf = POSUMConfiguration.newInstance();
        portfolioClient = new PolicyPortfolioClient();
        portfolioClient.init(posumConf);
        portfolioClient.start();
        YarnRPC rpc = YarnRPC.create(getConfig());
        InetSocketAddress masterServiceAddress = getConfig().getSocketAddr(
                POSUMConfiguration.META_BIND_ADDRESS,
                POSUMConfiguration.META_ADDRESS,
                POSUMConfiguration.DEFAULT_META_ADDRESS,
                POSUMConfiguration.DEFAULT_META_PORT);
        this.server =
                rpc.getServer(DataMasterProtocol.class, this, masterServiceAddress,
                        getConfig(), new DummyTokenSecretManager(),
                        getConfig().getInt(POSUMConfiguration.META_SERVICE_THREAD_COUNT,
                                POSUMConfiguration.DEFAULT_META_SERVICE_THREAD_COUNT));

        this.server.start();
        this.bindAddress = getConfig().updateConnectAddr(
                POSUMConfiguration.META_BIND_ADDRESS,
                POSUMConfiguration.META_ADDRESS,
                POSUMConfiguration.DEFAULT_META_ADDRESS,
                server.getListenerAddress());
    }

    /**
     * Series of abstract methods that need to be implemented by any scheduler
     */

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public synchronized void setConf(Configuration conf) {
        if (portfolioClient == null) {
            initializePortfolioLink();
        }
        setConf(conf);
        this.conf = conf;
    }

    @Override
    public void serviceInit(Configuration conf) throws Exception {
        initializePortfolioLink();
        portfolioClient.initScheduler(conf);
        super.serviceInit(conf);
    }

    @Override
    public void serviceStart() throws Exception {
        portfolioClient.startScheduler();
        super.serviceStart();
    }

    @Override
    public void serviceStop() throws Exception {
        portfolioClient.stopScheduler();
        if (this.server != null) {
            this.server.stop();
        }
        super.serviceStop();
    }

    @Override
    public int getNumClusterNodes() {
        return portfolioClient.getNumClusterNodes();
    }

    @Override
    public synchronized void setRMContext(RMContext rmContext) {
        this.rmContext = rmContext;
    }

    @Override
    public synchronized void reinitialize(Configuration conf, RMContext rmContext) throws IOException {
        this.rmContext = rmContext;
        this.conf = conf;
        portfolioClient.reinitScheduler(conf);
    }

    @Override
    public Allocation allocate(
            ApplicationAttemptId applicationAttemptId, List<ResourceRequest> ask,
            List<ContainerId> release, List<String> blacklistAdditions, List<String> blacklistRemovals) {
        return portfolioClient.allocateResources(applicationAttemptId, ask,
                release, blacklistAdditions, blacklistRemovals);
    }

    @Override
    public void handle(SchedulerEvent event) {
        portfolioClient.handleSchedulerEvent(event);
    }

    @Override
    public QueueInfo getQueueInfo(String queueName,
                                  boolean includeChildQueues, boolean recursive) {
        return portfolioClient.getSchedulerQueueInfo(queueName, false, includeChildQueues, recursive);
    }

    @Override
    public List<QueueUserACLInfo> getQueueUserAclInfo() {
        return null; //TODO DEFAULT_QUEUE.getQueueUserAclInfo(null);
    }

    @Override
    public ResourceCalculator getResourceCalculator() {
        return null; //TODO resourceCalculator;
    }

    @Override
    public void recover(RMStateStore.RMState state) {
        // NOT IMPLEMENTED
    }

    @Override
    public RMContainer getRMContainer(ContainerId containerId) {
        return null;
        //TODO
//        FiCaSchedulerApp attempt = getCurrentAttemptForContainer(containerId);
//        return (attempt == null) ? null : attempt.getRMContainer(containerId);
    }

    @Override
    protected void completedContainer(RMContainer rmContainer, ContainerStatus containerStatus, RMContainerEventType event) {
        //TODO forward
    }

    @Override
    public QueueMetrics getRootQueueMetrics() {
        return null; //TODO  DEFAULT_QUEUE.getMetrics();
    }

    @Override
    public synchronized boolean checkAccess(UserGroupInformation callerUGI,
                                            QueueACL acl, String queueName) {
        return false; //TODO DEFAULT_QUEUE.hasAccess(acl, callerUGI);
    }

    @Override
    public synchronized List<ApplicationAttemptId> getAppsInQueue(String queueName) {
        return null;
        //TODO forward
    }

    public Resource getUsedResource() {
        return null; //TODO usedResource;
    }

    /**
     * Series of public methods of the AbstractYarnScheduler that are overridden in order to forward ALL scheduling
     * functionality to the POSUM system (MONSTER TODO)
     */

    @Override
    public synchronized List<Container> getTransferredContainers(ApplicationAttemptId currentAttempt) {
        return super.getTransferredContainers(currentAttempt);
    }

    @Override
    public Map<ApplicationId, SchedulerApplication<SQSAppAttempt>> getSchedulerApplications() {
        return super.getSchedulerApplications();
    }

    @Override
    public Resource getClusterResource() {
        return super.getClusterResource();
    }

    @Override
    public Resource getMinimumResourceCapability() {
        return super.getMinimumResourceCapability();
    }

    @Override
    public Resource getMaximumResourceCapability() {
        return super.getMaximumResourceCapability();
    }

    @Override
    public Resource getMaximumResourceCapability(String queueName) {
        return super.getMaximumResourceCapability(queueName);
    }

    @Override
    public SQSAppAttempt getApplicationAttempt(ApplicationAttemptId applicationAttemptId) {
        return super.getApplicationAttempt(applicationAttemptId);
    }

    @Override
    public SchedulerAppReport getSchedulerAppInfo(ApplicationAttemptId appAttemptId) {
        return super.getSchedulerAppInfo(appAttemptId);
    }

    @Override
    public ApplicationResourceUsageReport getAppResourceUsageReport(ApplicationAttemptId appAttemptId) {
        return super.getAppResourceUsageReport(appAttemptId);
    }

    @Override
    public SQSAppAttempt getCurrentAttemptForContainer(ContainerId containerId) {
        return super.getCurrentAttemptForContainer(containerId);
    }

    @Override
    public SchedulerNodeReport getNodeReport(NodeId nodeId) {
        return super.getNodeReport(nodeId);
    }

    @Override
    public String moveApplication(ApplicationId appId, String newQueue) throws YarnException {
        return super.moveApplication(appId, newQueue);
    }

    @Override
    public void removeQueue(String queueName) throws YarnException {
        super.removeQueue(queueName);
    }

    @Override
    public void addQueue(Queue newQueue) throws YarnException {
        super.addQueue(newQueue);
    }

    @Override
    public void setEntitlement(String queue, QueueEntitlement entitlement) throws YarnException {
        super.setEntitlement(queue, entitlement);
    }

    @Override
    public synchronized void recoverContainersOnNode(List<NMContainerStatus> containerReports, RMNode nm) {
        super.recoverContainersOnNode(containerReports, nm);
    }

    @Override
    public SchedulerNode getSchedulerNode(NodeId nodeId) {
        return super.getSchedulerNode(nodeId);
    }

    @Override
    public synchronized void moveAllApps(String sourceQueue, String destQueue) throws YarnException {
        super.moveAllApps(sourceQueue, destQueue);
    }

    @Override
    public synchronized void killAllAppsInQueue(String queueName) throws YarnException {
        super.killAllAppsInQueue(queueName);
    }

    @Override
    public synchronized void updateNodeResource(RMNode nm, ResourceOption resourceOption) {
        super.updateNodeResource(nm, resourceOption);
    }

    @Override
    public EnumSet<YarnServiceProtos.SchedulerResourceTypes> getSchedulingResourceTypes() {
        return super.getSchedulingResourceTypes();
    }

    @Override
    public Set<String> getPlanQueues() throws YarnException {
        return super.getPlanQueues();
    }

    @Override
    public List<ResourceRequest> getPendingResourceRequestsForAttempt(ApplicationAttemptId attemptId) {
        return super.getPendingResourceRequestsForAttempt(attemptId);
    }

    /**
     * MetaSchedulerProtocol methods to handle communication from the POSUMMaster
     */

    @Override
    public SimpleResponse handleRMEvent(HandleRMEventRequest request) {
        Event event = request.getInterpretedEvent();
        rmContext.getDispatcher().getEventHandler().handle(event);
        //FIXME moveAllApps in AbstractYarnScheduler doesn't check for result, but maybe I should send it back?
        return SimpleResponse.newInstance(true);
    }
}
