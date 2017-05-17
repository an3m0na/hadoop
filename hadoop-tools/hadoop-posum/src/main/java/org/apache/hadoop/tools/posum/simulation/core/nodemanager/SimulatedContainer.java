package org.apache.hadoop.tools.posum.simulation.core.nodemanager;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.tools.posum.simulation.core.SimulationContext;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;

import java.util.List;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

@Private
@Unstable
public class SimulatedContainer implements Delayed {
  public static String AM_TYPE = "am";

  private ContainerId id;
  private Resource resource;
  private Long endTime;
  private Long lifeTime;
  private List<String> preferredLocations;
  private int priority;
  private String type;

  private SimulationContext simulationContext;
  private NodeId nodeId;
  private String taskId;

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
                            int priority,
                            String type,
                            String taskId,
                            List<String> preferredLocations) {
    this.simulationContext = simulationContext;
    this.resource = resource;
    this.lifeTime = lifeTime;
    this.priority = priority;
    this.type = type;
    this.taskId = taskId;
    this.preferredLocations = preferredLocations;
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

  public int getPriority() {
    return priority;
  }

  public String getType() {
    return type;
  }

  public void setPriority(int p) {
    priority = p;
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

  public void setTaskId(String taskId) {
    this.taskId = taskId;
  }

  public void setEndTime(long endTime) {
    this.endTime = endTime;
  }

  public List<String> getPreferredLocations() {
    return preferredLocations;
  }

  public void setPreferredLocations(List<String> preferredLocations) {
    this.preferredLocations = preferredLocations;
  }
}
