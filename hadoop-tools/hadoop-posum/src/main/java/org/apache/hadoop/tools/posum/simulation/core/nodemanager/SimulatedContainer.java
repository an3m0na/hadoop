package org.apache.hadoop.tools.posum.simulation.core.nodemanager;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.tools.posum.simulation.core.SimulationContext;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;

import java.util.List;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.tools.posum.common.util.cluster.ClusterUtils.DEFAULT_PRIORITY;

@Private
@Unstable
public class SimulatedContainer implements Delayed {
  public static String AM_TYPE = "am";

  private ContainerId id;
  private Resource resource;
  private Long endTime;
  private Long lifeTime;
  private List<String> preferredLocations;
  private Long originalStartTime;
  private Priority priority = DEFAULT_PRIORITY;
  private String type;

  private SimulationContext simulationContext;
  private NodeId nodeId;
  private String taskId;
  private String hostName;

  /**
   * invoked when AM schedules its container
   */
  public SimulatedContainer(SimulationContext simulationContext,
                            Resource resource,
                            String type,
                            NodeId nodeId,
                            ContainerId id) {
    this.simulationContext = simulationContext;
    this.resource = resource;
    this.type = type;
    this.nodeId = nodeId;
    this.id = id;
  }

  /**
   * invoked when AM schedules task containers to allocate
   */
  public SimulatedContainer(SimulationContext simulationContext,
                            Resource resource,
                            Long lifeTime,
                            String type,
                            String taskId,
                            List<String> preferredLocations,
                            Long originalStartTime,
                            String hostName) {
    this.simulationContext = simulationContext;
    this.resource = resource;
    this.lifeTime = lifeTime;
    this.type = type;
    this.taskId = taskId;
    this.preferredLocations = preferredLocations;
    this.originalStartTime = originalStartTime;
    this.hostName = hostName;
  }

  public Resource getResource() {
    return resource;
  }

  public ContainerId getId() {
    return id;
  }

  @Override
  public int compareTo(Delayed o) {
    if (!(o instanceof SimulatedContainer)) {
      throw new IllegalArgumentException(
        "Parameter must be a SimulatedContainer instance");
    }
    SimulatedContainer other = (SimulatedContainer) o;
    return (int) Math.signum(endTime - other.endTime);
  }

  @Override
  public long getDelay(TimeUnit unit) {
    return unit.convert(endTime - simulationContext.getCurrentTime(), TimeUnit.MILLISECONDS);
  }

  public Long getLifeTime() {
    return lifeTime;
  }

  public String getType() {
    return type;
  }

  public NodeId getNodeId() {
    return nodeId;
  }

  public void setNodeId(NodeId nodeId) {
    this.nodeId = nodeId;
  }

  public void setId(ContainerId id) {
    this.id = id;
  }

  public String getTaskId() {
    return taskId;
  }

  public void setEndTime(long endTime) {
    this.endTime = endTime;
  }

  public List<String> getPreferredLocations() {
    return preferredLocations;
  }

  public Long getOriginalStartTime() {
    return originalStartTime;
  }

  public String getHostName() {
    return hostName;
  }

  public void setHostName(String hostName) {
    this.hostName = hostName;
  }

  public Priority getPriority() {
    return priority;
  }

  public void setPriority(Priority priority) {
    this.priority = priority;
  }

  @Override
  public String toString() {
    return "SimulatedContainer{" +
      "id=" + id +
      ", resource=" + resource +
      ", endTime=" + endTime +
      ", lifeTime=" + lifeTime +
      ", preferredLocations=" + preferredLocations +
      ", originalStartTime=" + originalStartTime +
      ", priority=" + priority +
      ", type='" + type + '\'' +
      ", nodeId=" + nodeId +
      ", taskId='" + taskId + '\'' +
      ", hostName='" + hostName + '\'' +
      '}';
  }
}
