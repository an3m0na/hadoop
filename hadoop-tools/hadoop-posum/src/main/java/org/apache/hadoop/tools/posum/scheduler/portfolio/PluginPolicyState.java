package org.apache.hadoop.tools.posum.scheduler.portfolio;

import org.apache.hadoop.tools.posum.scheduler.portfolio.singleq.SQSQueue;
import org.apache.hadoop.tools.posum.scheduler.portfolio.singleq.SQSchedulerNode;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplication;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;

import java.util.Map;

public class PluginPolicyState {
  private final Resource usedResource;
  private final SQSQueue queue;
  private final Map<NodeId, ? extends SQSchedulerNode> nodes;
  private final Map<ApplicationId, ? extends SchedulerApplication<? extends SchedulerApplicationAttempt>> applications;
  private final Resource clusterResource;
  private final Resource maxAllocation;
  private final boolean usePortForNodeName;

  public PluginPolicyState(Resource usedResource,
                           SQSQueue queue,
                           Map<NodeId, ? extends SQSchedulerNode> nodes,
                           Map<ApplicationId, ? extends SchedulerApplication<? extends SchedulerApplicationAttempt>> applications,
                           Resource clusterResource,
                           Resource maxAllocation,
                           boolean usePortForNodeName) {
    this.usedResource = usedResource;
    this.queue = queue;
    this.nodes = nodes;
    this.applications = applications;
    this.clusterResource = clusterResource;
    this.maxAllocation = maxAllocation;
    this.usePortForNodeName = usePortForNodeName;
  }

  public Resource getUsedResource() {
    return usedResource;
  }

  public SQSQueue getQueue() {
    return queue;
  }

  public Map<NodeId, ? extends SQSchedulerNode> getNodes() {
    return nodes;
  }

  public Map<ApplicationId, ? extends SchedulerApplication<? extends SchedulerApplicationAttempt>> getApplications() {
    return applications;
  }

  public Resource getClusterResource() {
    return clusterResource;
  }

  public Resource getMaxAllocation() {
    return maxAllocation;
  }

  public boolean isUsePortForNodeName() {
    return usePortForNodeName;
  }

  public static class Builder {
    private Resource usedResource;
    private SQSQueue queue;
    private Map<NodeId, ? extends SQSchedulerNode> nodes;
    private Map<ApplicationId, ? extends SchedulerApplication<? extends SchedulerApplicationAttempt>> applications;
    private Resource clusterResource;
    private Resource maxAllocation;
    private boolean usePortForNodeName;

    public Builder usedResource(Resource usedResource) {
      this.usedResource = usedResource;
      return this;
    }

    public Builder queue(SQSQueue queue) {
      this.queue = queue;
      return this;
    }

    public Builder nodes(Map<NodeId, ? extends SQSchedulerNode> nodes) {
      this.nodes = nodes;
      return this;
    }

    public Builder applications(Map<ApplicationId, ? extends SchedulerApplication<? extends SchedulerApplicationAttempt>> applications) {
      this.applications = applications;
      return this;
    }

    public Builder clusterResource(Resource clusterResource) {
      this.clusterResource = clusterResource;
      return this;
    }

    public Builder maxAllocation(Resource maxAllocation) {
      this.maxAllocation = maxAllocation;
      return this;
    }

    public Builder usePortForNodeName(boolean usePortForNodeName) {
      this.usePortForNodeName = usePortForNodeName;
      return this;
    }

    public PluginPolicyState build() {
      return new PluginPolicyState(
        usedResource,
        queue,
        nodes,
        applications,
        clusterResource,
        maxAllocation,
        usePortForNodeName
      );
    }
  }

  public static Builder builder() {
    return new Builder();
  }
}
