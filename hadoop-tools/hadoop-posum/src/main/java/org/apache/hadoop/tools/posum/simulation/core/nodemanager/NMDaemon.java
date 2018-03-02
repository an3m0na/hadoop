package org.apache.hadoop.tools.posum.simulation.core.nodemanager;

import org.apache.hadoop.tools.posum.common.util.cluster.NMCore;
import org.apache.hadoop.tools.posum.simulation.core.SimulationContext;
import org.apache.hadoop.tools.posum.simulation.core.daemon.WorkerDaemon;
import org.apache.hadoop.tools.posum.simulation.core.dispatcher.ContainerEvent;
import org.apache.hadoop.tools.posum.simulation.core.dispatcher.ContainerEventType;
import org.apache.hadoop.tools.posum.simulation.predictor.TaskPredictionInput;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.records.NodeAction;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.util.Records;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.DelayQueue;

import static org.apache.hadoop.tools.posum.simulation.core.nodemanager.SimulatedContainer.AM_TYPE;

public class NMDaemon extends WorkerDaemon {
  // containers with various STATE
  private final List<ContainerId> completedContainerList;
  private final List<ContainerId> releasedContainerList;
  private DelayQueue<SimulatedContainer> containerQueue;
  private Map<ContainerId, SimulatedContainer> runningContainers;
  private final List<ContainerId> amContainerList;
  private final static Logger LOG = Logger.getLogger(NMDaemon.class);
  private NMCore core;

  public NMDaemon(SimulationContext simulationContext) {
    super(simulationContext);
    completedContainerList =
      Collections.synchronizedList(new ArrayList<ContainerId>());
    releasedContainerList =
      Collections.synchronizedList(new ArrayList<ContainerId>());
    amContainerList =
      Collections.synchronizedList(new ArrayList<ContainerId>());
  }

  public void init(String hostname, int memory, int cores,
                   int dispatchTime, int heartBeatInterval, ResourceManager rm) {
    super.init(dispatchTime, heartBeatInterval);
    // create resource
    this.core = new NMCore(rm, simulationContext.getTopologyProvider().resolve(hostname), hostname, memory, cores);
    // init data structures
    containerQueue = new DelayQueue<>();
    runningContainers = new ConcurrentHashMap<>();
    simulationContext.getDispatcher().register(ContainerEventType.class, new EventHandler<ContainerEvent>() {
      @Override
      public void handle(ContainerEvent event) {
        if (!event.getContainer().getNodeId().equals(core.getNodeId()))
          return;
        switch (event.getType()) {
          case CONTAINER_STARTED:
            addNewContainer(event.getContainer());
            break;
          case CONTAINER_FINISHED:
            if (AM_TYPE.equals(event.getContainer().getType()))
              cleanupContainer(event.getContainer().getId());
            break;
        }
      }
    });
  }

  @Override
  public void doFirstStep() throws IOException, YarnException {
    core.registerWithRM();
  }

  @Override
  public void doStep() throws Exception {
    // we check the lifetime for each running containers
    SimulatedContainer cs;
    synchronized (completedContainerList) {
      while ((cs = containerQueue.poll()) != null) {
        runningContainers.remove(cs.getId());
        completedContainerList.add(cs.getId());
        LOG.trace(MessageFormat.format("T={0}: Container {1} has completed", simulationContext.getCurrentTime(),
          cs.getId()));
      }
    }

    core.prepareHeartBeat(generateContainerStatusList());
    synchronized (simulationContext) {
      simulationContext.setAwaitingScheduler(true);
    }
    NodeHeartbeatResponse beatResponse = core.sendHeartBeat();
    if (!beatResponse.getContainersToCleanup().isEmpty()) {
      // remove from queue
      synchronized (releasedContainerList) {
        for (ContainerId containerId : beatResponse.getContainersToCleanup()) {
          if (amContainerList.contains(containerId)) {
            // AM container (not killed?, only release)
            synchronized (amContainerList) {
              amContainerList.remove(containerId);
            }
            LOG.trace(MessageFormat.format("T={0}: NodeManager {1} releases " +
              "an AM ({2}).", simulationContext.getCurrentTime(), core.getNodeId(), containerId));
          } else {
            cs = runningContainers.remove(containerId);
            containerQueue.remove(cs);
            releasedContainerList.add(containerId);
            LOG.trace(MessageFormat.format("T={0}: NodeManager {1} releases a " +
              "container ({2}).", simulationContext.getCurrentTime(), core.getNodeId(), containerId));
          }
        }
      }
    }
    if (beatResponse.getNodeAction() == NodeAction.SHUTDOWN) {
      cleanUp();
    }
  }

  @Override
  public void cleanUp() {
    // do nothing
  }

  @Override
  public boolean isFinished() {
    return false;
  }

  /**
   * catch status of all containers located on current node
   */
  private ArrayList<ContainerStatus> generateContainerStatusList() {
    ArrayList<ContainerStatus> csList = new ArrayList<>();
    // add running containers
    for (SimulatedContainer container : runningContainers.values()) {
      csList.add(newContainerStatus(container.getId(),
        ContainerState.RUNNING, ContainerExitStatus.SUCCESS));
    }
    synchronized (amContainerList) {
      for (ContainerId cId : amContainerList) {
        csList.add(newContainerStatus(cId,
          ContainerState.RUNNING, ContainerExitStatus.SUCCESS));
      }
    }
    // add complete containers
    synchronized (completedContainerList) {
      for (ContainerId cId : completedContainerList) {
        LOG.trace(MessageFormat.format("T={0}: NodeManager {1} completed" +
          " container ({2}).", simulationContext.getCurrentTime(), core.getNodeId(), cId));
        csList.add(newContainerStatus(
          cId, ContainerState.COMPLETE, ContainerExitStatus.SUCCESS));
      }
      completedContainerList.clear();
    }
    // released containers
    synchronized (releasedContainerList) {
      for (ContainerId cId : releasedContainerList) {
        LOG.trace(MessageFormat.format("T={0}: NodeManager {1} released container" +
          " ({2}).", simulationContext.getCurrentTime(), simulationContext.getCurrentTime(), core.getNodeId(), cId));
        csList.add(newContainerStatus(
          cId, ContainerState.COMPLETE, ContainerExitStatus.ABORTED));
      }
      releasedContainerList.clear();
    }
    return csList;
  }

  private ContainerStatus newContainerStatus(ContainerId cId,
                                             ContainerState state,
                                             int exitState) {
    ContainerStatus cs = Records.newRecord(ContainerStatus.class);
    cs.setContainerId(cId);
    cs.setState(state);
    cs.setExitStatus(exitState);
    return cs;
  }

  public NodeId getNodeId() {
    return core.getNodeId();
  }

  /**
   * launch a new container with the given life time
   */
  private void addNewContainer(SimulatedContainer container) {
    LOG.trace(MessageFormat.format("T={0}: NodeManager {1} launches a new " +
      "container ({2}).", simulationContext.getCurrentTime(), core.getNodeId(), container.getId()));
    if (AM_TYPE.equals(container.getType())) {
      // AM container
      synchronized (amContainerList) {
        amContainerList.add(container.getId());
      }
    } else {
      // normal container
      Long lifeTimeMS = container.getLifeTime();
      if (lifeTimeMS == null) {
        TaskPredictionInput predictionInput = new TaskPredictionInput(container.getTask(), getNodeId().getHost());
        lifeTimeMS = simulationContext.getPredictor().predictTaskBehavior(predictionInput).getDuration();
      }
      if (simulationContext.isOnlineSimulation() && container.getOriginalStartTime() != null)
        lifeTimeMS -= simulationContext.getCurrentTime() - container.getOriginalStartTime();
      container.setEndTime(simulationContext.getCurrentTime() + lifeTimeMS);
      containerQueue.add(container);
      runningContainers.put(container.getId(), container);
    }
  }

  /**
   * clean up an AM container and add to completed list
   *
   * @param containerId id of the container to be cleaned
   */
  private void cleanupContainer(ContainerId containerId) {
    synchronized (amContainerList) {
      amContainerList.remove(containerId);
    }
    synchronized (completedContainerList) {
      completedContainerList.add(containerId);
    }
  }
}
