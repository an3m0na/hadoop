package org.apache.hadoop.tools.posum.core.scheduler.meta;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.tools.posum.common.util.POSUMConfiguration;
import org.apache.hadoop.tools.posum.common.util.POSUMException;
import org.apache.hadoop.tools.posum.core.scheduler.portfolio.DataOrientedPolicy;
import org.apache.hadoop.tools.posum.core.scheduler.portfolio.PluginPolicy;
import org.apache.hadoop.tools.posum.core.scheduler.portfolio.singleq.SQSAppAttempt;
import org.apache.hadoop.tools.posum.core.scheduler.portfolio.singleq.SQSchedulerNode;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.*;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.*;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;

import java.io.IOException;
import java.util.List;

/**
 * Created by ane on 2/4/16.
 */
public class PortfolioMetaScheduler extends
        AbstractYarnScheduler<SQSAppAttempt, SQSchedulerNode> implements
        Configurable {

    private static Log logger = LogFactory.getLog(PortfolioMetaScheduler.class);

    private Configuration conf;
    private Configuration posumConf;
    private MetaSchedulerCommService commService;

    //TODO choose default some other way
    private Class<? extends PluginPolicy> currentSchedulerClass = DataOrientedPolicy.class;
    private PluginPolicy currentScheduler;


    public PortfolioMetaScheduler() {
        super(PortfolioMetaScheduler.class.getName());
    }


    private void initScheduler() {
        if (currentScheduler == null) {
            try {
                currentScheduler = currentSchedulerClass.newInstance();
            } catch (InstantiationException | IllegalAccessException e) {
                throw new POSUMException("Could not instantiate scheduler for class " + currentSchedulerClass, e);
            }
        }
        currentScheduler.initializePlugin(posumConf);
        currentScheduler.init(conf);
        //TODO think about when switching policies
    }

    private void initComm() {
        commService = new MetaSchedulerCommService();
        commService.init(posumConf);
        commService.start();
    }

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
        this.posumConf = POSUMConfiguration.newInstance();
        setConf(conf);
        initComm();
        initScheduler();
        super.serviceInit(conf);
    }

    @Override
    public void serviceStart() throws Exception {
        currentScheduler.start();
        super.serviceStart();
    }

    @Override
    public void serviceStop() throws Exception {
        if (this.commService != null) {
            this.commService.stop();
        }
        if (this.currentScheduler != null) {
            this.currentScheduler.stop();
        }
        super.serviceStop();
    }

    @Override
    public int getNumClusterNodes() {
        return currentScheduler.getNumClusterNodes();
    }

    @Override
    public synchronized void setRMContext(RMContext rmContext) {
        this.rmContext = rmContext;
        currentScheduler.setRMContext(rmContext);
    }

    @Override
    public synchronized void reinitialize(Configuration conf, RMContext rmContext) throws IOException {
        this.rmContext = rmContext;
        this.conf = conf;
        currentScheduler.reinitialize(conf, rmContext);
    }

    @Override
    public Allocation allocate(
            ApplicationAttemptId applicationAttemptId, List<ResourceRequest> ask,
            List<ContainerId> release, List<String> blacklistAdditions, List<String> blacklistRemovals) {
        return currentScheduler.allocate(applicationAttemptId, ask,
                release, blacklistAdditions, blacklistRemovals);
    }

    @Override
    public void handle(SchedulerEvent event) {
        currentScheduler.handle(event);
    }

    @Override
    public QueueInfo getQueueInfo(String queueName,
                                  boolean includeChildQueues, boolean recursive) throws IOException {
        return currentScheduler.getQueueInfo(queueName, includeChildQueues, recursive);
    }

    @Override
    public List<QueueUserACLInfo> getQueueUserAclInfo() {
        return currentScheduler.getQueueUserAclInfo();
    }

    @Override
    public ResourceCalculator getResourceCalculator() {
        return currentScheduler.getResourceCalculator();
    }

    @Override
    public void recover(RMStateStore.RMState state) throws Exception {
        currentScheduler.recover(state);
    }

    @Override
    public RMContainer getRMContainer(ContainerId containerId) {
        return currentScheduler.getRMContainer(containerId);
    }

    @Override
    protected void completedContainer(RMContainer rmContainer, ContainerStatus containerStatus, RMContainerEventType event) {
        currentScheduler.forwardCompletedContainer(rmContainer, containerStatus, event);
    }

    @Override
    public QueueMetrics getRootQueueMetrics() {
        return currentScheduler.getRootQueueMetrics();
    }

    @Override
    public synchronized boolean checkAccess(UserGroupInformation callerUGI,
                                            QueueACL acl, String queueName) {
        return currentScheduler.checkAccess(callerUGI, acl, queueName);
    }

    @Override
    public synchronized List<ApplicationAttemptId> getAppsInQueue(String queueName) {
        return currentScheduler.getAppsInQueue(queueName);
    }

}
