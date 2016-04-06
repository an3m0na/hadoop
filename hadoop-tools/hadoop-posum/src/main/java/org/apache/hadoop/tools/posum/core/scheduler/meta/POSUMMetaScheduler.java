package org.apache.hadoop.tools.posum.core.scheduler.meta;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.tools.posum.core.scheduler.portfolio.singleq.SQSAppAttempt;
import org.apache.hadoop.tools.posum.core.scheduler.portfolio.singleq.SQSchedulerNode;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.*;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;

import java.io.IOException;
import java.util.List;

/**
 * Created by ane on 2/4/16.
 */
public class POSUMMetaScheduler extends
        AbstractYarnScheduler<SQSAppAttempt, SQSchedulerNode> implements
        Configurable {

    private static Log logger = LogFactory.getLog(POSUMMetaScheduler.class);

    Configuration conf;

    public POSUMMetaScheduler() {
        super(POSUMMetaScheduler.class.getName());
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public synchronized void setConf(Configuration conf) {
        //TODO forward
        this.conf = conf;
    }

    @Override
    public void serviceInit(Configuration conf) throws Exception {
        //TODO initScheduler(conf);
        super.serviceInit(conf);
    }

    @Override
    public void serviceStart() throws Exception {
        super.serviceStart();
    }

    @Override
    public void serviceStop() throws Exception {
        super.serviceStop();
    }

    @Override
    public int getNumClusterNodes() {
        return nodes.size();
    }

    @Override
    public synchronized void setRMContext(RMContext rmContext) {
        this.rmContext = rmContext;
    }

    @Override
    public synchronized void reinitialize(Configuration conf, RMContext rmContext) throws IOException {
        //TODO forward  (usually setConf(conf));
    }

    @Override
    public Allocation allocate(
            ApplicationAttemptId applicationAttemptId, List<ResourceRequest> ask,
            List<ContainerId> release, List<String> blacklistAdditions, List<String> blacklistRemovals) {
        //TODO forward
        return null;
    }

    @Override
    public void handle(SchedulerEvent event) {
        //TODO forward
    }

    @Override
    public QueueInfo getQueueInfo(String queueName,
                                  boolean includeChildQueues, boolean recursive) {
        return null; //TODO DEFAULT_QUEUE.getQueueInfo(false, false);
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
        //TODO
//        if (queueName.equals(DEFAULT_QUEUE.getQueueName())) {
//            List<ApplicationAttemptId> attempts =
//                    new ArrayList<ApplicationAttemptId>(applications.size());
//            for (SchedulerApplication<FiCaSchedulerApp> app : applications.values()) {
//                attempts.add(app.getCurrentAppAttempt().getApplicationAttemptId());
//            }
//            return attempts;
//        } else {
//            return null;
//        }
    }

    public Resource getUsedResource() {
        return null; //TODO usedResource;
    }


}
