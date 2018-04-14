package org.apache.hadoop.tools.posum.simulation.core.appmaster;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.tools.posum.common.util.PosumException;
import org.apache.hadoop.tools.posum.simulation.core.SimulationContext;
import org.apache.hadoop.tools.posum.simulation.core.dispatcher.ContainerEvent;
import org.apache.hadoop.tools.posum.simulation.core.nodemanager.SimulatedContainer;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.Priority;
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

  public static final Priority PRIORITY_REDUCE = Priority.newInstance(10);
  public static final Priority PRIORITY_MAP = Priority.newInstance(20);

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
  private Map<String, LinkedList<SimulatedContainer>> scheduledMapsByLocation = new HashMap<>();

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
    for (SimulatedContainer container : initialContainers) {
      if (container.getType().equals(TaskType.MAP.name())) {
        container.setPriority(PRIORITY_MAP);
        pendingMaps.add(container);
      } else if (container.getType().equals(TaskType.REDUCE.name())) {
        container.setPriority(PRIORITY_REDUCE);
        pendingReduces.add(container);
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
  protected void processResponseQueue() throws InterruptedException, YarnException, IOException {
    while (!responseQueue.isEmpty()) {
      AllocateResponse response = responseQueue.take();
      if (!isAMContainerRunning) {
        // Check whether receive the am container
        if (response == null || response.getAllocatedContainers().isEmpty())
          continue;
        // Get AM container
        Container container = response.getAllocatedContainers().get(0);
        LOG.trace(MessageFormat.format("Sim={0} T={1}: App {2} gets AM container", simulationContext.getSchedulerClass().getSimpleName(), simulationContext.getCurrentTime(), getAppId(), container));
        // Start AM container
        amContainer = new SimulatedContainer(
          simulationContext,
          container.getResource(),
          AM_TYPE,
          container.getNodeId(),
          container.getId()
        );
        simulationContext.getDispatcher().getEventHandler().handle(new ContainerEvent(CONTAINER_STARTED, amContainer));
        LOG.trace(MessageFormat.format("T={0}: Application {1} starts its " +
          "AM container ({2}).", simulationContext.getCurrentTime(), core.getAppId(), amContainer.getId()));
        isAMContainerRunning = true;
        return;
      }

      // check completed containers
      if (!response.getCompletedContainersStatuses().isEmpty()) {
        for (ContainerStatus cs : response.getCompletedContainersStatuses()) {
          ContainerId containerId = cs.getContainerId();
          if (cs.getExitStatus() == ContainerExitStatus.SUCCESS) {
            if (assignedMaps.containsKey(containerId)) {
              LOG.trace(MessageFormat.format("T={0}: Application {1} has one " +
                  "mapper finished ({2}).", simulationContext.getCurrentTime(), core.getAppId(), containerId));
              SimulatedContainer simulatedContainer = assignedMaps.remove(containerId);
              mapFinished++;
              finishedContainers++;
              simulationContext.getDispatcher().getEventHandler().handle(new ContainerEvent(CONTAINER_FINISHED, simulatedContainer));
            } else if (assignedReduces.containsKey(containerId)) {
              LOG.trace(MessageFormat.format("T={0}: Application {1} has one " +
                "reducer finished ({2}).", simulationContext.getCurrentTime(), core.getAppId(), containerId));
              SimulatedContainer simulatedContainer = assignedReduces.remove(containerId);
              reduceFinished++;
              finishedContainers++;
              simulationContext.getDispatcher().getEventHandler().handle(new ContainerEvent(CONTAINER_FINISHED, simulatedContainer));
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
            } else if (amContainer.getId().equals(containerId)) {
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
        simulationContext.getDispatcher().getEventHandler().handle(new ContainerEvent(CONTAINER_FINISHED, amContainer));
        isAMContainerRunning = false;
        LOG.trace(MessageFormat.format("T={0}: Application {1} sends out event " +
          "to clean up its AM container.", simulationContext.getCurrentTime(), core.getAppId()));
        isFinished = true;
      }

      if (LOG.isTraceEnabled() && !response.getAllocatedContainers().isEmpty())
        LOG.trace(MessageFormat.format("Sim={0} T={1}: App {2} gets containers {3}", simulationContext.getSchedulerClass().getSimpleName(), simulationContext.getCurrentTime(), getAppId(), response.getAllocatedContainers()));

      for (Container container : response.getAllocatedContainers()) {
        if (simulationContext.isOnlineSimulation()) {
          // check pre-assigned containers
          if (startPreAssigned(container))
            continue;
          if (preAssignedRemaining()) // container was not pre-assigned but there are pre-assignments waiting
            throw new PosumException(MessageFormat.format("Sim={0} T={1}: Unexpected container during pre-assignments: {2}",
                simulationContext.getSchedulerClass().getSimpleName(), simulationContext.getCurrentTime(), container));
        }
        // assign regular container
        assignContainer(container);
      }
    }
  }

  private boolean startPreAssigned(Container container) {
    for (Iterator<SimulatedContainer> iterator = (isForMap(container) ? scheduledMaps : scheduledReduces).iterator(); iterator.hasNext(); ) {
      SimulatedContainer scheduledContainer = iterator.next();
      if (scheduledContainer.getHostName() != null) {
        if (scheduledContainer.getHostName().equals(container.getNodeId().getHost())) {
          if (isForMap(container))
            unscheduleMap(scheduledContainer);
          else
            iterator.remove();
          LOG.trace(MessageFormat.format("Sim={0} T={1}: App {2} is pre-assigned container ", simulationContext.getSchedulerClass().getSimpleName(), simulationContext.getCurrentTime(), getAppId(), container));
          startContainer(container, scheduledContainer);
          return true;
        }
      } else return false;
    }
    return false;
  }

  private boolean isForMap(Container container) {
    return container.getPriority().equals(PRIORITY_MAP);
  }

  private void unscheduleMap(SimulatedContainer scheduledMap) {
    for (LinkedList<SimulatedContainer> list : scheduledMapsByLocation.values())
      list.remove(scheduledMap);
    scheduledMaps.remove(scheduledMap);
  }

  private void startContainer(Container container,
                              SimulatedContainer simulatedContainer) {
    simulatedContainer.setNodeId(container.getNodeId());
    simulatedContainer.setId(container.getId());
    if (isForMap(container))
      assignedMaps.put(container.getId(), simulatedContainer);
    else
      assignedReduces.put(container.getId(), simulatedContainer);
    removeRequests(simulatedContainer, container.getPriority());
    simulationContext.getDispatcher().getEventHandler().handle(new ContainerEvent(CONTAINER_STARTED, simulatedContainer));
  }

  private boolean preAssignedRemaining() {
    return (!scheduledMaps.isEmpty() && scheduledMaps.getFirst().getHostName() != null) ||
      (!scheduledReduces.isEmpty() && scheduledReduces.getFirst().getHostName() != null);
  }

  private void assignContainer(Container container) {
    SimulatedContainer assigned = isForMap(container) ? assignContainerToMap(container) : scheduledReduces.poll();
    if (assigned != null)
      startContainer(container, assigned);
    else
      releaseContainer(container);
  }

  private SimulatedContainer assignContainerToMap(Container allocatedContainer) {
    String host = allocatedContainer.getNodeId().getHost();
    String rack = simulationContext.getTopologyProvider().getRack(host);
    LinkedList<SimulatedContainer> list = scheduledMapsByLocation.get(host);
    // try to assign to all nodes first to match node local
    if (list != null && !list.isEmpty()) {
      SimulatedContainer scheduledMap = list.remove();
      unscheduleMap(scheduledMap);
      return scheduledMap;
    }
    // then rack local
    list = scheduledMapsByLocation.get(rack);
    if (list != null && !list.isEmpty()) {
      SimulatedContainer scheduledMap = list.remove();
      unscheduleMap(scheduledMap);
      return scheduledMap;
    }
    // off-switch
    SimulatedContainer scheduledMap = scheduledMaps.peek();
    unscheduleMap(scheduledMap);
    return scheduledMap;
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
  protected void sendContainerRequests() throws Exception {
    if (isFinished) {
      return;
    }

    if (isAMContainerRunning) {
      if (mapFinished != mapTotal) {
        // map phase
        if (!pendingMaps.isEmpty()) {
          createRequests(pendingMaps, PRIORITY_MAP);
          LOG.trace(MessageFormat.format("T={0}: Application {1} sends out " +
            "request for {2} mappers.", simulationContext.getCurrentTime(), core.getAppId(), pendingMaps.size()));
          scheduleMaps(pendingMaps);
          pendingMaps.clear();
        } else if (!pendingFailedMaps.isEmpty() && scheduledMaps.isEmpty()) {
          createRequests(pendingFailedMaps, PRIORITY_MAP);
          LOG.trace(MessageFormat.format("T={0}: Application {1} sends out " +
              "requests for {2} failed mappers.", simulationContext.getCurrentTime(), core.getAppId(),
            pendingFailedMaps.size()));
          scheduleMaps(pendingFailedMaps);
          pendingFailedMaps.clear();
        }
      } else if (reduceStarted() && reduceFinished != reduceTotal) {
        // reduce phase
        if (!pendingReduces.isEmpty()) {
          createRequests(pendingReduces, PRIORITY_REDUCE);
          LOG.trace(MessageFormat.format("T={0}: Application {1} sends out " +
            "requests for {2} reducers.", simulationContext.getCurrentTime(), core.getAppId(), pendingReduces.size()));
          scheduledReduces.addAll(pendingReduces);
          pendingReduces.clear();
        } else if (!pendingFailedReduces.isEmpty()
          && scheduledReduces.isEmpty()) {
          createRequests(pendingFailedReduces, PRIORITY_REDUCE);
          LOG.trace(MessageFormat.format("T={0}: Application {1} sends out " +
              "request for {2} failed reducers.", simulationContext.getCurrentTime(), simulationContext.getCurrentTime(), core.getAppId(),
            pendingFailedReduces.size()));
          scheduledReduces.addAll(pendingFailedReduces);
          pendingFailedReduces.clear();
        }
      }
    }
    super.sendContainerRequests();
  }

  private void scheduleMaps(LinkedList<SimulatedContainer> pendingMaps) {
    for (SimulatedContainer pendingMap : pendingMaps) {
      scheduledMaps.add(pendingMap);
      List<String> locations = new ArrayList<>(pendingMap.getPreferredLocations());
      locations.addAll(simulationContext.getTopologyProvider().getRacks(locations));
      for (String location : locations) {
        LinkedList<SimulatedContainer> list = scheduledMapsByLocation.get(location);
        if (list == null) {
          list = new LinkedList<>();
          scheduledMapsByLocation.put(location, list);
        }
        list.add(pendingMap);
      }
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
