package org.apache.hadoop.tools.posum.simulation.core.nodemanager;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.tools.posum.common.records.dataentity.TaskProfile;
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
  private String type;
  private TaskProfile task;
  private Long lifeTime;
  private Long originalStartTime;
  private Priority priority = DEFAULT_PRIORITY;

  private SimulationContext simulationContext;
  private NodeId nodeId;

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
    this.nodeId = nodeId;
    this.id = id;
    this.type = type;
  }

  /**
   * invoked when AM schedules task containers to allocate
   */
  public SimulatedContainer(SimulationContext simulationContext,
                            Resource resource,
                            TaskProfile task,
                            Long lifeTime,
                            Long originalStartTime) {
    this.simulationContext = simulationContext;
    this.resource = resource;
    this.task = task;
    this.lifeTime = lifeTime;
    this.originalStartTime = originalStartTime;
    this.type = task.getType().name();
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
    if (task == null)
      return null;
    return task.getId();
  }

  public void setEndTime(long endTime) {
    this.endTime = endTime;
  }

  public List<String> getPreferredLocations() {
    if (task == null)
      return null;
    return task.getSplitLocations();
  }

  public Long getOriginalStartTime() {
    return originalStartTime;
  }

  public String getHostName() {
    if (task == null)
      return null;
    return task.getHostName();
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
      ", originalStartTime=" + originalStartTime +
      ", priority=" + priority +
      ", nodeId=" + nodeId +
      '}';
  }

  public TaskProfile getTask() {
    return task;
  }
}
