package org.apache.hadoop.tools.posum.simulation.core.appmaster;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.tools.posum.common.util.PosumException;
import org.apache.hadoop.tools.posum.simulation.core.SimulationContext;
import org.apache.hadoop.tools.posum.simulation.core.dispatcher.ContainerEvent;
import org.apache.hadoop.tools.posum.simulation.core.nodemanager.SimulatedContainer;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.tools.posum.simulation.core.dispatcher.ContainerEventType.CONTAINER_FINISHED;
import static org.apache.hadoop.tools.posum.simulation.core.dispatcher.ContainerEventType.CONTAINER_STARTED;
import static org.apache.hadoop.tools.posum.simulation.core.nodemanager.SimulatedContainer.AM_TYPE;

@Private
@Unstable
public class MRAMDaemon extends AMDaemon {
  /*
  Vocabulary Used: 
  pending -> requests which are NOT yet sent to RM
  scheduled -> requests which are sent to RM but not yet assigned
  assigned -> requests which are assigned to a container
  completed -> request corresponding to which container has completed
  
  Maps are scheduled as soon as their requests are received. Reduces are
  scheduled when all maps have finished (not support slow-start currently).
  */

  public static final int PRIORITY_REDUCE = 10;
  public static final int PRIORITY_MAP = 20;

  private LinkedList<SimulatedContainer> pendingMaps = new LinkedList<>();
  private LinkedList<SimulatedContainer> pendingFailedMaps = new LinkedList<>();
  private LinkedList<SimulatedContainer> scheduledMaps = new LinkedList<>();
  private Map<ContainerId, SimulatedContainer> assignedMaps = new HashMap<>();
  private LinkedList<SimulatedContainer> pendingReduces = new LinkedList<>();
  private LinkedList<SimulatedContainer> pendingFailedReduces = new LinkedList<>();
  private LinkedList<SimulatedContainer> scheduledReduces = new LinkedList<>();
  private Map<ContainerId, SimulatedContainer> assignedReduces = new HashMap<>();
  private LinkedList<SimulatedContainer> allMaps = new LinkedList<>();
  private LinkedList<SimulatedContainer> allReduces = new LinkedList<>();

  // counters
  private int mapFinished = 0;
  private int mapTotal = 0;
  private int reduceFinished = 0;
  private int reduceTotal = 0;
  // waiting for AM container
  private boolean isAMContainerRunning = false;
  private SimulatedContainer amContainer;
  // finished
  private boolean isFinished = false;

  public final Logger LOG = Logger.getLogger(MRAMDaemon.class);
  private float slowStartRatio;

  public MRAMDaemon(SimulationContext simulationContext) {
    super(simulationContext);
  }

  public void init(int heartbeatInterval, List<SimulatedContainer> containerList, ResourceManager rm,
                   long traceStartTime, String user, String queue, String oldAppId, String originalHostName, float slowStartRatio) {
    super.init(heartbeatInterval, containerList, rm, traceStartTime, user, queue, oldAppId, originalHostName);
    amType = "mapreduce";

    // get map/reduce tasks
    for (SimulatedContainer cs : initialContainers) {
      if (cs.getType().equals("map")) {
        cs.setPriority(PRIORITY_MAP);
        pendingMaps.add(cs);
      } else if (cs.getType().equals("reduce")) {
        cs.setPriority(PRIORITY_REDUCE);
        pendingReduces.add(cs);
      }
    }
    allMaps.addAll(pendingMaps);
    allReduces.addAll(pendingReduces);
    mapTotal = pendingMaps.size();
    reduceTotal = pendingReduces.size();
    totalContainers = mapTotal + reduceTotal;
    this.slowStartRatio = slowStartRatio;
  }

  @Override
  protected void processResponseQueue()
    throws InterruptedException, YarnException, IOException {
    LOG.trace(MessageFormat.format("Sim={0} T={1}: App {2} gets containers {3}", simulationContext.getSchedulerClass().getSimpleName(), simulationContext.getCurrentTime(), getAppId(), !responseQueue.isEmpty()? responseQueue.peek().getAllocatedContainers() : "no containers"));

    // Check whether receive the am container
    if (!isAMContainerRunning) {
      if (!responseQueue.isEmpty()) {
        AllocateResponse response = responseQueue.take();
        if (response != null
          && !response.getAllocatedContainers().isEmpty()) {
          // Get AM container
          Container container = response.getAllocatedContainers().get(0);
          // Start AM container
          amContainer = new SimulatedContainer(
            simulationContext,
            container.getResource(),
            AM_TYPE,
            container.getNodeId(),
            container.getId()
          );
          simulationContext.getDispatcher().getEventHandler()
            .handle(new ContainerEvent(CONTAINER_STARTED, amContainer));
          LOG.trace(MessageFormat.format("T={0}: Application {1} starts its " +
            "AM container ({2}).", simulationContext.getCurrentTime(), core.getAppId(), amContainer.getId()));
          isAMContainerRunning = true;
        }
      }
      return;
    }

    while (!responseQueue.isEmpty()) {
      AllocateResponse response = responseQueue.take();

      // check completed containers
      if (!response.getCompletedContainersStatuses().isEmpty()) {
        for (ContainerStatus cs : response.getCompletedContainersStatuses()) {
          ContainerId containerId = cs.getContainerId();
          if (cs.getExitStatus() == ContainerExitStatus.SUCCESS) {
            if (assignedMaps.containsKey(containerId)) {
              LOG.trace(MessageFormat.format("T={0}: Application {1} has one " +
                "mapper finished ({2}).", core.getAppId(), containerId));
              SimulatedContainer simulatedContainer = assignedMaps.remove(containerId);
              mapFinished++;
              finishedContainers++;
              simulationContext.getDispatcher().getEventHandler()
                .handle(new ContainerEvent(CONTAINER_FINISHED, simulatedContainer));
            } else if (assignedReduces.containsKey(containerId)) {
              LOG.trace(MessageFormat.format("T={0}: Application {1} has one " +
                "reducer finished ({2}).", simulationContext.getCurrentTime(), core.getAppId(), containerId));
              SimulatedContainer simulatedContainer = assignedReduces.remove(containerId);
              reduceFinished++;
              finishedContainers++;
              simulationContext.getDispatcher().getEventHandler()
                .handle(new ContainerEvent(CONTAINER_FINISHED, simulatedContainer));
            } else {
              // am container released event
              isFinished = true;
              LOG.info(MessageFormat.format("T={0}: Application {1} goes to " +
                "cleanUp.", simulationContext.getCurrentTime(), core.getAppId()));
            }
          } else {
            // container to be killed
            if (assignedMaps.containsKey(containerId)) {
              LOG.trace(MessageFormat.format("T={0}: Application {1} has one " +
                "mapper killed ({2}).", core.getAppId(), containerId));
              pendingFailedMaps.add(assignedMaps.remove(containerId));
            } else if (assignedReduces.containsKey(containerId)) {
              LOG.trace(MessageFormat.format("T={0}: Application {1} has one " +
                "reducer killed ({2}).", simulationContext.getCurrentTime(), core.getAppId(), containerId));
              pendingFailedReduces.add(assignedReduces.remove(containerId));
            } else {
              LOG.info(MessageFormat.format("T={0}: Application {1}'s AM is " +
                "going to be killed. Restarting...", simulationContext.getCurrentTime(), core.getAppId()));
              restart();
            }
          }
        }
      }

      // check finished
      if (isAMContainerRunning &&
        (mapFinished == mapTotal) &&
        (reduceFinished == reduceTotal)) {
        // to release the AM container
        simulationContext.getDispatcher().getEventHandler()
          .handle(new ContainerEvent(CONTAINER_FINISHED, amContainer));
        isAMContainerRunning = false;
        LOG.trace(MessageFormat.format("T={0}: Application {1} sends out event " +
          "to clean up its AM container.", simulationContext.getCurrentTime(), core.getAppId()));
        isFinished = true;
      }

      // check allocated containers
      for (Container container : response.getAllocatedContainers()) {
        if (simulationContext.isOnlineSimulation()) {
          if (checkPreAssigned(container, scheduledMaps, assignedMaps))
            continue;
          if (checkPreAssigned(container, scheduledReduces, assignedReduces))
            continue;
          if (preAssignedRemaining()) // container was not pre-assigned but there are pre-assignments waiting
            throw new PosumException(MessageFormat.format("Sim={0} T={1}: Unexpected container during pre-assignments: {2} on {3}",
              simulationContext.getSchedulerClass().getSimpleName(), simulationContext.getCurrentTime(), container.getId(), container.getNodeId()));
        }
        if (!scheduledMaps.isEmpty()) {
          startContainer(container, scheduledMaps.remove(), assignedMaps);
        } else if (!this.scheduledReduces.isEmpty()) {
          startContainer(container, scheduledReduces.remove(), assignedReduces);
        }
      }
    }
  }

  private boolean preAssignedRemaining() {
    return (!scheduledMaps.isEmpty() && scheduledMaps.getFirst().getHostName() != null) ||
      (!scheduledReduces.isEmpty() && scheduledReduces.getFirst().getHostName() != null);
  }

  private boolean checkPreAssigned(Container container,
                                   LinkedList<SimulatedContainer> scheduled,
                                   Map<ContainerId, SimulatedContainer> assigned) {
    for (Iterator<SimulatedContainer> iterator = scheduled.iterator(); iterator.hasNext(); ) {
      SimulatedContainer simulatedContainer = iterator.next();
      if (simulatedContainer.getHostName() != null) {
        if (simulatedContainer.getHostName().equals(container.getNodeId().getHost())) {
          startContainer(container, simulatedContainer, assigned);
          iterator.remove();
          return true;
        }
      } else return false;
    }
    return false;
  }

  private void startContainer(Container container,
                              SimulatedContainer simulatedContainer,
                              Map<ContainerId, SimulatedContainer> assigned) {
    simulatedContainer.setNodeId(container.getNodeId());
    simulatedContainer.setId(container.getId());
    assigned.put(container.getId(), simulatedContainer);
    simulationContext.getDispatcher().getEventHandler().handle(new ContainerEvent(CONTAINER_STARTED, simulatedContainer));
  }

  @Override
  void restart()
    throws YarnException, IOException, InterruptedException {
    // clear
    finishedContainers = 0;
    isFinished = false;
    mapFinished = 0;
    reduceFinished = 0;
    pendingFailedMaps.clear();
    pendingMaps.clear();
    pendingReduces.clear();
    pendingFailedReduces.clear();
    pendingMaps.addAll(allMaps);
    pendingReduces.addAll(allReduces);
    isAMContainerRunning = false;
    amContainer = null;
    super.restart();
  }

  @Override
  protected void sendContainerRequest()
    throws YarnException, IOException, InterruptedException {
    if (isFinished) {
      return;
    }

    // send out request
    List<ResourceRequest> ask = null;
    if (isAMContainerRunning) {
      if (mapFinished != mapTotal) {
        // map phase
        if (!pendingMaps.isEmpty()) {
          ask = packageRequests(pendingMaps, PRIORITY_MAP);
          LOG.trace(MessageFormat.format("T={0}: Application {1} sends out " +
            "request for {2} mappers.", simulationContext.getCurrentTime(), core.getAppId(), pendingMaps.size()));
          scheduledMaps.addAll(pendingMaps);
          pendingMaps.clear();
        } else if (!pendingFailedMaps.isEmpty() && scheduledMaps.isEmpty()) {
          ask = packageRequests(pendingFailedMaps, PRIORITY_MAP);
          LOG.trace(MessageFormat.format("T={0}: Application {1} sends out " +
              "requests for {2} failed mappers.", simulationContext.getCurrentTime(), core.getAppId(),
            pendingFailedMaps.size()));
          scheduledMaps.addAll(pendingFailedMaps);
          pendingFailedMaps.clear();
        }
      } else if (reduceStarted() && reduceFinished != reduceTotal) {
        // reduce phase
        if (!pendingReduces.isEmpty()) {
          ask = packageRequests(pendingReduces, PRIORITY_REDUCE);
          LOG.trace(MessageFormat.format("T={0}: Application {1} sends out " +
            "requests for {2} reducers.", simulationContext.getCurrentTime(), core.getAppId(), pendingReduces.size()));
          scheduledReduces.addAll(pendingReduces);
          pendingReduces.clear();
        } else if (!pendingFailedReduces.isEmpty()
          && scheduledReduces.isEmpty()) {
          ask = packageRequests(pendingFailedReduces, PRIORITY_REDUCE);
          LOG.trace(MessageFormat.format("T={0}: Application {1} sends out " +
              "request for {2} failed reducers.", simulationContext.getCurrentTime(), simulationContext.getCurrentTime(), core.getAppId(),
            pendingFailedReduces.size()));
          scheduledReduces.addAll(pendingFailedReduces);
          pendingFailedReduces.clear();
        }
      }
    }
    if (ask == null) {
      ask = new ArrayList<>();
    }

    final AllocateRequest request = core.createAllocateRequest(ask);
    if (totalContainers == 0) {
      request.setProgress(1.0f);
    } else {
      request.setProgress((float) finishedContainers / totalContainers);
    }

    AllocateResponse response = core.sendAllocateRequest(request);
    if (response != null) {
      responseQueue.put(response);
    }
  }

  private boolean reduceStarted() {
    return mapTotal == 0 || 1.0 * mapFinished / mapTotal >= slowStartRatio;
  }

  @Override
  public void cleanUp() throws Exception {
    super.cleanUp();

    // clear data structures
    allMaps.clear();
    allReduces.clear();
    assignedMaps.clear();
    assignedReduces.clear();
    pendingFailedMaps.clear();
    pendingFailedReduces.clear();
    pendingMaps.clear();
    pendingReduces.clear();
    scheduledMaps.clear();
    scheduledReduces.clear();
    responseQueue.clear();
  }

  @Override
  public boolean isFinished() {
    return isFinished;
  }
}
