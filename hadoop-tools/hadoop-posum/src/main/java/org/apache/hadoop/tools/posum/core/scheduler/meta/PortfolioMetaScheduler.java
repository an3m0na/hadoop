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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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
    private Map<String, Class<? extends PluginPolicy>> policies;

    private Class<? extends PluginPolicy> currentPolicyClass = DefaultPolicy.FIFO.implClass;
    private PluginPolicy currentPolicy;
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
        currentPolicy.init(conf);
    }

    private void transferState(PluginPolicy oldPolicy) {
        //TODO transfer everything
        if (isInState(STATE.STARTED))
            currentPolicy.start();
    }

    protected void changeToPolicy(String policyName) {
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
        }

    }

    private void initComm() {
        commService = new MetaSchedulerCommService(this);
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
        preparePolicies();
        initComm();
        initPolicy();
        super.serviceInit(conf);
    }

    @Override
    public void serviceStart() throws Exception {
        readLock.lock();
        try {
            currentPolicy.start();
        } finally {
            readLock.unlock();
        }
        super.serviceStart();
    }

    @Override
    public void serviceStop() throws Exception {
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
        super.serviceStop();
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

}
