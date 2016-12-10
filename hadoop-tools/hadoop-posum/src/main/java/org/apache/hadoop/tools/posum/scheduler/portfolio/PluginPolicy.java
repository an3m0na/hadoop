package org.apache.hadoop.tools.posum.scheduler.portfolio;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.scheduler.core.MetaSchedulerCommService;
import org.apache.hadoop.tools.posum.scheduler.portfolio.singleq.SQSAppAttempt;
import org.apache.hadoop.tools.posum.scheduler.portfolio.singleq.SQSQueue;
import org.apache.hadoop.tools.posum.scheduler.portfolio.singleq.SQSchedulerNode;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplication;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;

import java.util.List;
import java.util.Map;

public abstract class PluginPolicy<
        A extends SchedulerApplicationAttempt,
        N extends SchedulerNode>
        extends AbstractYarnScheduler<A, N> implements Configurable {

    protected Class<A> aClass;
    protected Class<N> nClass;
    protected Configuration pluginConf;
    protected MetaSchedulerCommService commService;

    public PluginPolicy(Class<A> aClass, Class<N> nClass, String policyName) {
        super(policyName);
        this.aClass = aClass;
        this.nClass = nClass;
    }

    protected static class PluginPolicyState {
        public final Resource usedResource;
        public final SQSQueue queue;
        public final Map<NodeId, ? extends SQSchedulerNode> nodes;
        public final Map<ApplicationId, ? extends SchedulerApplication<? extends SQSAppAttempt>> applications;
        public final Resource clusterResource;
        public final Resource maxAllocation;
        public final boolean usePortForNodeName;

        public PluginPolicyState(Resource usedResource,
                                 SQSQueue queue,
                                 Map<NodeId, ? extends SQSchedulerNode> nodes,
                                 Map<ApplicationId, ? extends SchedulerApplication<? extends SQSAppAttempt>> applications,
                                 Resource clusterResource,
                                 Resource maxAllocation, boolean usePortForNodeName) {
            this.usedResource = usedResource;
            this.queue = queue;
            this.nodes = nodes;
            this.applications = applications;
            this.clusterResource = clusterResource;
            this.maxAllocation = maxAllocation;
            this.usePortForNodeName = usePortForNodeName;
        }
    }

    public void initializePlugin(Configuration conf, MetaSchedulerCommService commService) {
        this.pluginConf = conf;
        this.commService = commService;
    }

    public void forwardCompletedContainer(RMContainer rmContainer, ContainerStatus containerStatus, RMContainerEventType event) {
        completedContainer(rmContainer, containerStatus, event);
    }

    public void forwardInitMaximumResourceCapability(Resource maximumAllocation) {
        initMaximumResourceCapability(maximumAllocation);
    }

    public synchronized void forwardContainerLaunchedOnNode(ContainerId containerId, SchedulerNode node) {
        containerLaunchedOnNode(containerId, node);
    }

    public void forwardRecoverResourceRequestForContainer(RMContainer rmContainer) {
        super.recoverResourceRequestForContainer(rmContainer);
    }

    public void forwardCreateReleaseCache() {
        createReleaseCache();
    }

    public void forwardReleaseContainers(List<ContainerId> containers, SchedulerApplicationAttempt attempt) {
        releaseContainers(containers, attempt);
    }

    public void forwardUpdateMaximumAllocation(SchedulerNode node, boolean add) {
        updateMaximumAllocation(node, add);
    }

    public void forwardRefreshMaximumAllocation(Resource newMaxAlloc) {
        refreshMaximumAllocation(newMaxAlloc);
    }

    public abstract void transferStateFromPolicy(PluginPolicy other);
}
