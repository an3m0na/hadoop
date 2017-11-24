package org.apache.hadoop.tools.posum.scheduler.portfolio;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplication;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;

import java.util.Map;

public class PluginPolicyState<N extends SchedulerNode & PluginSchedulerNode> {
  private final Resource clusterResource;
  private final Map<NodeId, N> nodes;
  private final Map<ApplicationId, ? extends SchedulerApplication<? extends SchedulerApplicationAttempt>> applications;

  public PluginPolicyState(Resource clusterResource,
                           Map<NodeId, N> nodes,
                           Map<ApplicationId, ? extends SchedulerApplication<? extends SchedulerApplicationAttempt>> applications) {
    this.clusterResource = clusterResource;
    this.nodes = nodes;
    this.applications = applications;
  }

  public Resource getClusterResource() {
    return clusterResource;
  }

  public Map<NodeId, N> getNodes() {
    return nodes;
  }

  public Map<ApplicationId, ? extends SchedulerApplication<? extends SchedulerApplicationAttempt>> getApplications() {
    return applications;
  }
}
