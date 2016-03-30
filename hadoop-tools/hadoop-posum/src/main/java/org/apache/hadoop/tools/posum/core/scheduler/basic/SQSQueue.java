package org.apache.hadoop.tools.posum.core.scheduler.basic;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.tools.posum.common.POSUMException;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.lang.reflect.Constructor;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by ane on 1/22/16.
 */
public class SQSQueue implements Queue {

    private final List<SQSAppAttempt> apps = new ArrayList<>();
    // get a lock with fair distribution for app list updates
    private final ReadWriteLock rwl = new ReentrantReadWriteLock(true);
    private final Lock readLock = rwl.readLock();
    private final Lock writeLock = rwl.writeLock();

    private static final RecordFactory recordFactory =
            RecordFactoryProvider.getRecordFactory(null);


    private final String name;
    private QueueMetrics metrics;
    private SingleQueueScheduler scheduler;
    private ActiveUsersManager activeUsersManager;

    public SQSQueue(String name, SingleQueueScheduler scheduler) {
        this.name = name;
        this.scheduler = scheduler;
        this.metrics = QueueMetrics.forQueue(this.name, null, false, scheduler.getConf());
        this.activeUsersManager = new ActiveUsersManager(metrics);
    }

    static <Q extends SQSQueue> Q getInstance(Class<Q> qClass, String name, SingleQueueScheduler scheduler) {
        try {
            Constructor<Q> constructor = qClass.getConstructor(String.class, SingleQueueScheduler.class);
            return constructor.newInstance(name, scheduler);
        } catch (Exception e) {
            throw new POSUMException("Failed to instantiate scheduler queue via default constructor" + e);
        }
    }

    @Override
    public String getQueueName() {
        return name;
    }

    @Override
    public QueueMetrics getMetrics() {
        // Just like FIFO (FS has more complicated fairness metrics also)
        return metrics;
    }

    @Override
    public QueueInfo getQueueInfo(boolean includeChildQueues, boolean recursive) {
        // Just like FIFO (FS uses fair share based ratios)
        QueueInfo queueInfo = recordFactory.newRecordInstance(QueueInfo.class);
        queueInfo.setQueueName(name);
        queueInfo.setCapacity(1.0f);
        int totalMemory = scheduler.getClusterResource().getMemory();
        if (totalMemory == 0) {
            queueInfo.setCurrentCapacity(0.0f);
        } else {
            queueInfo.setCurrentCapacity((float) scheduler.getUsedResource().getMemory()
                    / totalMemory);
        }
        queueInfo.setMaximumCapacity(1.0f);
        queueInfo.setChildQueues(new ArrayList<QueueInfo>());
        queueInfo.setQueueState(QueueState.RUNNING);
        return queueInfo;
    }

    @Override
    public List<QueueUserACLInfo> getQueueUserAclInfo(UserGroupInformation userGroupInformation) {
        // Just like FIFO (FS does something complex because of user queue permissions)
        QueueUserACLInfo queueUserAclInfo =
                recordFactory.newRecordInstance(QueueUserACLInfo.class);
        queueUserAclInfo.setQueueName(name);
        queueUserAclInfo.setUserAcls(Arrays.asList(QueueACL.values()));
        return Collections.singletonList(queueUserAclInfo);
    }

    @Override
    public boolean hasAccess(QueueACL queueACL, UserGroupInformation userGroupInformation) {
        // Shorthand for FIFO (FS does something complex because of user queue permissions)
        return true;
    }

    @Override
    public ActiveUsersManager getActiveUsersManager() {
        // Nobody does anything special here
        return activeUsersManager;
    }

    @Override
    public void recoverContainer(Resource resource, SchedulerApplicationAttempt schedulerAttempt, RMContainer rmContainer) {
// Just like FIFO
        if (rmContainer.getState().equals(RMContainerState.COMPLETED)) {
            return;
        }
        scheduler.increaseUsedResources(rmContainer);
        scheduler.updateAppHeadRoom(schedulerAttempt);
        metrics.setAvailableResourcesToQueue(Resources.subtract(scheduler.getClusterResource(),
                scheduler.getUsedResource()));
    }

    @Override
    public Set<String> getAccessibleNodeLabels() {
        // Nobody implements these
        return null;
    }

    @Override
    public String getDefaultNodeLabelExpression() {
        // Nobody implements these
        return null;
    }

    public void setAvailableResourcesToQueue(Resource resource) {
       metrics.setAvailableResourcesToQueue(resource);
    }

    public void deactivateApplication(String user, ApplicationId applicationId) {
        activeUsersManager.deactivateApplication(user, applicationId);
    }
}
