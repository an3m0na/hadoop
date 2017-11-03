package org.apache.hadoop.tools.posum.scheduler.portfolio;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.common.records.call.StoreLogCall;
import org.apache.hadoop.tools.posum.common.records.dataentity.LogEntry;
import org.apache.hadoop.tools.posum.common.util.communication.DatabaseProvider;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNodeReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;

import java.util.List;
import java.util.Map;

import static org.apache.hadoop.tools.posum.common.util.Utils.DEFAULT_PRIORITY;

public abstract class PluginPolicy<
  A extends SchedulerApplicationAttempt & PluginApplicationAttempt,
  N extends SchedulerNode & PluginSchedulerNode>
  extends AbstractYarnScheduler<A, N> implements Configurable {

  protected Class<A> aClass;
  protected Class<N> nClass;
  protected DatabaseProvider dbProvider;

  public PluginPolicy(Class<A> aClass, Class<N> nClass, String policyName) {
    super(policyName);
    this.aClass = aClass;
    this.nClass = nClass;
  }

  public void initializePlugin(Configuration conf, DatabaseProvider dbProvider) {
    setConf(conf);
    this.dbProvider = dbProvider;
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

  @Override
  public void handle(SchedulerEvent event) {
    switch (event.getType()) {
      case NODE_ADDED: {
        NodeAddedSchedulerEvent nodeAddedEvent = (NodeAddedSchedulerEvent) event;
        dbProvider.getDatabase().execute(StoreLogCall.newInstance(LogEntry.Type.NODE_ADD,
          nodeAddedEvent.getAddedRMNode().getHostName()));
      }
      break;
      case NODE_REMOVED: {
        NodeRemovedSchedulerEvent nodeRemovedEvent = (NodeRemovedSchedulerEvent) event;
        dbProvider.getDatabase().execute(StoreLogCall.newInstance(LogEntry.Type.NODE_REMOVE,
          nodeRemovedEvent.getRemovedRMNode().getHostName()));
      }
    }
  }

  public void transferStateFromPolicy(PluginPolicy other) {
    importState(other.exportState());
  }

  protected abstract PluginPolicyState exportState();

  protected abstract <T extends SchedulerNode & PluginSchedulerNode> void importState(PluginPolicyState<T> state);

  public abstract boolean forceContainerAssignment(ApplicationId appId, String hostName, Priority priority);

  public boolean forceContainerAssignment(ApplicationId appId, String hostName) {
    return forceContainerAssignment(appId, hostName, DEFAULT_PRIORITY);
  }

  public abstract Map<String, SchedulerNodeReport> getNodeReports();
}
