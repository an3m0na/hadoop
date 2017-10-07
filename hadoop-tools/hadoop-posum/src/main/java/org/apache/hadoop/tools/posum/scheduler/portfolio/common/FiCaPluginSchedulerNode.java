package org.apache.hadoop.tools.posum.scheduler.portfolio.common;

import org.apache.hadoop.tools.posum.common.util.PosumException;
import org.apache.hadoop.tools.posum.common.util.Utils;
import org.apache.hadoop.tools.posum.scheduler.portfolio.PluginSchedulerNode;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;

import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Set;

public class FiCaPluginSchedulerNode extends FiCaSchedulerNode implements PluginSchedulerNode {

  private final SchedulerNode core;

  public FiCaPluginSchedulerNode(RMNode node, boolean usePortForNodeName, Set<String> nodeLabels) {
    super(node, usePortForNodeName, nodeLabels);
    this.core = null;
  }

  public <T extends SchedulerNode & PluginSchedulerNode> FiCaPluginSchedulerNode(T predecessor) {
    super(predecessor.getRMNode(), true, predecessor.getLabels()); // not used
    this.core = predecessor.getCoreNode() == null ? predecessor : predecessor.getCoreNode();
  }

  public SchedulerNode getCoreNode() {
    return core;
  }

  public static <N extends FiCaPluginSchedulerNode> N getInstance(Class<N> nClass,
                                                                  RMNode node,
                                                                  boolean usePortForNodeName,
                                                                  Set<String> nodeLabels) {
    try {
      Constructor<N> constructor = nClass.getConstructor(RMNode.class, boolean.class, Set.class);
      return constructor.newInstance(node, usePortForNodeName, nodeLabels);
    } catch (Exception e) {
      throw new PosumException("Failed to instantiate scheduler node via constructor", e);
    }
  }

  public static <T extends SchedulerNode & PluginSchedulerNode, N extends FiCaPluginSchedulerNode> N getInstance(Class<N> nClass,
                                                                                                                 T node) {
    try {
      Constructor<N> constructor = nClass.getConstructor(SchedulerNode.class);
      return constructor.newInstance(node);
    } catch (Exception e) {
      throw new PosumException("Failed to instantiate scheduler node via constructor", e);
    }
  }

  @Override
  public void reserveResource(SchedulerApplicationAttempt application, Priority priority, RMContainer container) {
    if (core != null) {
      core.reserveResource(application, priority, container);
      return;
    }
    super.reserveResource(application, priority, container);
  }

  @Override
  public void unreserveResource(SchedulerApplicationAttempt application) {
    if (core != null) {
      core.unreserveResource(application);
      return;
    }
    super.unreserveResource(application);
  }

  @Override
  public RMNode getRMNode() {
    if (core != null) {
      return core.getRMNode();
    }
    return super.getRMNode();
  }

  @Override
  public synchronized void setTotalResource(Resource resource) {
    if (core != null) {
      core.setTotalResource(resource);
      return;
    }
    super.setTotalResource(resource);
  }

  @Override
  public NodeId getNodeID() {
    if (core != null) {
      return core.getNodeID();
    }
    return super.getNodeID();
  }

  @Override
  public String getHttpAddress() {
    if (core != null) {
      return core.getHttpAddress();
    }
    return super.getHttpAddress();
  }

  @Override
  public String getNodeName() {
    if (core != null) {
      return core.getNodeName();
    }
    return super.getNodeName();
  }

  @Override
  public String getRackName() {
    if (core != null) {
      return core.getRackName();
    }
    return super.getRackName();
  }

  @Override
  public synchronized void allocateContainer(RMContainer rmContainer) {
    if (core != null) {
      core.allocateContainer(rmContainer);
      return;
    }
    super.allocateContainer(rmContainer);
  }

  @Override
  public synchronized Resource getAvailableResource() {
    if (core != null) {
      return core.getAvailableResource();
    }
    return super.getAvailableResource();
  }

  @Override
  public synchronized Resource getUsedResource() {
    if (core != null) {
      return core.getUsedResource();
    }
    return super.getUsedResource();
  }

  @Override
  public synchronized Resource getTotalResource() {
    if (core != null) {
      return core.getTotalResource();
    }
    return super.getTotalResource();
  }

  @Override
  public synchronized boolean isValidContainer(ContainerId containerId) {
    if (core != null) {
      return core.isValidContainer(containerId);
    }
    return super.isValidContainer(containerId);
  }

  @Override
  public synchronized void releaseContainer(Container container) {
    if (core != null) {
      core.releaseContainer(container);
      return;
    }
    super.releaseContainer(container);
  }

  @Override
  public String toString() {
    if (core != null) {
      return core.toString();
    }
    return super.toString();
  }

  @Override
  public int getNumContainers() {
    if (core != null) {
      return core.getNumContainers();
    }
    return super.getNumContainers();
  }

  @Override
  public synchronized List<RMContainer> getRunningContainers() {
    if (core != null) {
      return core.getRunningContainers();
    }
    return super.getRunningContainers();
  }

  @Override
  public synchronized RMContainer getReservedContainer() {
    if (core != null) {
      return core.getReservedContainer();
    }
    return super.getReservedContainer();
  }

  @Override
  protected synchronized void setReservedContainer(RMContainer reservedContainer) {
    if (core != null) {
      Utils.invokeMethod(core, SchedulerNode.class, "setReservedContainer", new Class<?>[]{RMContainer.class}, reservedContainer);
      return;
    }
    super.setReservedContainer(reservedContainer);
  }

  @Override
  public synchronized void recoverContainer(RMContainer rmContainer) {
    if (core != null) {
      core.recoverContainer(rmContainer);
      return;
    }
    super.recoverContainer(rmContainer);
  }

  @Override
  public Set<String> getLabels() {
    if (core != null) {
      return core.getLabels();
    }
    return super.getLabels();
  }

  @Override
  public void updateLabels(Set<String> labels) {
    if (core != null) {
      core.updateLabels(labels);
      return;
    }
    super.updateLabels(labels);
  }
}
