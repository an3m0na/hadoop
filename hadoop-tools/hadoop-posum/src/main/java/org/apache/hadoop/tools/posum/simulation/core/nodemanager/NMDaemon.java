package org.apache.hadoop.tools.posum.simulation.core.nodemanager;

import org.apache.hadoop.tools.posum.simulation.core.SimulationContext;
import org.apache.hadoop.tools.posum.simulation.core.daemon.WorkerDaemon;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.NodeAction;
import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
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

public class NMDaemon extends WorkerDaemon {
  // node resource
  private RMNode node;
  // master key
  private MasterKey masterKey;
  // containers with various STATE
  private List<ContainerId> completedContainerList;
  private List<ContainerId> releasedContainerList;
  private DelayQueue<SimulatedContainer> containerQueue;
  private Map<ContainerId, SimulatedContainer> runningContainers;
  private List<ContainerId> amContainerList;
  // resource manager
  private ResourceManager rm;
  // heart beat response id
  private int RESPONSE_ID = 1;
  private final static Logger LOG = Logger.getLogger(NMDaemon.class);

  public NMDaemon(SimulationContext simulationContext) {
    super(simulationContext);
  }

  public void init(String rack, String hostname, int memory, int cores,
                   int dispatchTime, int heartBeatInterval, ResourceManager rm) {
    super.init(dispatchTime, heartBeatInterval);
    // create resource
    this.node = NodeInfo.newNodeInfo(rack, hostname, BuilderUtils.newResource(memory, cores));
    this.rm = rm;
    // init data structures
    completedContainerList =
      Collections.synchronizedList(new ArrayList<ContainerId>());
    releasedContainerList =
      Collections.synchronizedList(new ArrayList<ContainerId>());
    containerQueue = new DelayQueue<SimulatedContainer>();
    amContainerList =
      Collections.synchronizedList(new ArrayList<ContainerId>());
    runningContainers =
      new ConcurrentHashMap<ContainerId, SimulatedContainer>();

  }

  @Override
  public void doFirstStep() throws IOException, YarnException {
    // register NM with RM
    RegisterNodeManagerRequest req =
      Records.newRecord(RegisterNodeManagerRequest.class);
    req.setNodeId(node.getNodeID());
    req.setResource(node.getTotalCapability());
    req.setHttpPort(80);
    RegisterNodeManagerResponse response = null;
    response = rm.getResourceTrackerService().registerNodeManager(req);
    masterKey = response.getNMTokenMasterKey();
  }

  @Override
  public void doStep() throws Exception {
    // we check the lifetime for each running containers
    SimulatedContainer cs = null;
    synchronized (completedContainerList) {
      while ((cs = containerQueue.poll()) != null) {
        runningContainers.remove(cs.getId());
        completedContainerList.add(cs.getId());
        LOG.debug(MessageFormat.format("Container {0} has completed",
          cs.getId()));
      }
    }

    // send heart beat
    NodeHeartbeatRequest beatRequest =
      Records.newRecord(NodeHeartbeatRequest.class);
    beatRequest.setLastKnownNMTokenMasterKey(masterKey);
    NodeStatus ns = Records.newRecord(NodeStatus.class);

    ns.setContainersStatuses(generateContainerStatusList());
    ns.setNodeId(node.getNodeID());
    ns.setKeepAliveApplications(new ArrayList<ApplicationId>());
    ns.setResponseId(RESPONSE_ID++);
    ns.setNodeHealthStatus(NodeHealthStatus.newInstance(true, "", 0));
    beatRequest.setNodeStatus(ns);
    NodeHeartbeatResponse beatResponse =
      rm.getResourceTrackerService().nodeHeartbeat(beatRequest);
    if (!beatResponse.getContainersToCleanup().isEmpty()) {
      // remove from queue
      synchronized (releasedContainerList) {
        for (ContainerId containerId : beatResponse.getContainersToCleanup()) {
          if (amContainerList.contains(containerId)) {
            // AM container (not killed?, only release)
            synchronized (amContainerList) {
              amContainerList.remove(containerId);
            }
            LOG.debug(MessageFormat.format("NodeManager {0} releases " +
              "an AM ({1}).", node.getNodeID(), containerId));
          } else {
            cs = runningContainers.remove(containerId);
            containerQueue.remove(cs);
            releasedContainerList.add(containerId);
            LOG.debug(MessageFormat.format("NodeManager {0} releases a " +
              "container ({1}).", node.getNodeID(), containerId));
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
    ArrayList<ContainerStatus> csList = new ArrayList<ContainerStatus>();
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
        LOG.debug(MessageFormat.format("NodeManager {0} completed" +
          " container ({1}).", node.getNodeID(), cId));
        csList.add(newContainerStatus(
          cId, ContainerState.COMPLETE, ContainerExitStatus.SUCCESS));
      }
      completedContainerList.clear();
    }
    // released containers
    synchronized (releasedContainerList) {
      for (ContainerId cId : releasedContainerList) {
        LOG.debug(MessageFormat.format("NodeManager {0} released container" +
          " ({1}).", node.getNodeID(), cId));
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

  public RMNode getNode() {
    return node;
  }

  /**
   * launch a new container with the given life time
   */
  public void addNewContainer(Container container, long lifeTimeMS) {
    LOG.debug(MessageFormat.format("NodeManager {0} launches a new " +
      "container ({1}).", node.getNodeID(), container.getId()));
    if (lifeTimeMS != -1) {
      // normal container
      SimulatedContainer cs = new SimulatedContainer(simulationContext, container.getId(),
        container.getResource(), lifeTimeMS + simulationContext.getCurrentTime(),
        lifeTimeMS);
      containerQueue.add(cs);
      runningContainers.put(cs.getId(), cs);
    } else {
      // AM container
      // -1 means AMContainer
      synchronized (amContainerList) {
        amContainerList.add(container.getId());
      }
    }
  }

  /**
   * clean up an AM container and add to completed list
   *
   * @param containerId id of the container to be cleaned
   */
  public void cleanupContainer(ContainerId containerId) {
    synchronized (amContainerList) {
      amContainerList.remove(containerId);
    }
    synchronized (completedContainerList) {
      completedContainerList.add(containerId);
    }
  }
}
