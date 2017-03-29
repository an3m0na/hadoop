package org.apache.hadoop.tools.posum.simulation.core.appmaster;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.tools.posum.simulation.core.SimulationContext;
import org.apache.hadoop.tools.posum.simulation.core.nodemanager.SimulatedContainer;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

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

  private static final int PRIORITY_REDUCE = 10;
  private static final int PRIORITY_MAP = 20;

  // pending maps
  private LinkedList<SimulatedContainer> pendingMaps =
    new LinkedList<SimulatedContainer>();

  // pending failed maps
  private LinkedList<SimulatedContainer> pendingFailedMaps =
    new LinkedList<SimulatedContainer>();

  // scheduled maps
  private LinkedList<SimulatedContainer> scheduledMaps =
    new LinkedList<SimulatedContainer>();

  // assigned maps
  private Map<ContainerId, SimulatedContainer> assignedMaps =
    new HashMap<ContainerId, SimulatedContainer>();

  // reduces which are not yet scheduled
  private LinkedList<SimulatedContainer> pendingReduces =
    new LinkedList<SimulatedContainer>();

  // pending failed reduces
  private LinkedList<SimulatedContainer> pendingFailedReduces =
    new LinkedList<SimulatedContainer>();

  // scheduled reduces
  private LinkedList<SimulatedContainer> scheduledReduces =
    new LinkedList<SimulatedContainer>();

  // assigned reduces
  private Map<ContainerId, SimulatedContainer> assignedReduces =
    new HashMap<ContainerId, SimulatedContainer>();

  // all maps & reduces
  private LinkedList<SimulatedContainer> allMaps =
    new LinkedList<SimulatedContainer>();
  private LinkedList<SimulatedContainer> allReduces =
    new LinkedList<SimulatedContainer>();

  // counters
  private int mapFinished = 0;
  private int mapTotal = 0;
  private int reduceFinished = 0;
  private int reduceTotal = 0;
  // waiting for AM container
  private boolean isAMContainerRunning = false;
  private Container amContainer;
  // finished
  private boolean isFinished = false;
  // resource for AM container
  private final static int MR_AM_CONTAINER_RESOURCE_MEMORY_MB = 1024;
  private final static int MR_AM_CONTAINER_RESOURCE_VCORES = 1;

  public final Logger LOG = Logger.getLogger(MRAMDaemon.class);

  public MRAMDaemon(SimulationContext simulationContext) {
    super(simulationContext);
  }

  public void init(int heartbeatInterval, List<SimulatedContainer> containerList, ResourceManager rm,
                   long traceStartTime, String user, String queue, String oldAppId) {
    super.init(heartbeatInterval, containerList, rm, traceStartTime, user, queue, oldAppId);
    amtype = "mapreduce";

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
  }

  @Override
  public void doFirstStep() throws Exception {
    super.doFirstStep();

    requestAMContainer();
  }

  /**
   * send out request for AM container
   */
  protected void requestAMContainer()
    throws YarnException, IOException, InterruptedException {
    List<ResourceRequest> ask = new ArrayList<ResourceRequest>();
    ResourceRequest amRequest = createResourceRequest(
      BuilderUtils.newResource(MR_AM_CONTAINER_RESOURCE_MEMORY_MB,
        MR_AM_CONTAINER_RESOURCE_VCORES),
      ResourceRequest.ANY, 1, 1);
    ask.add(amRequest);
    LOG.debug(MessageFormat.format("Application {0} sends out allocate " +
      "request for its AM", appId));
    final AllocateRequest request = this.createAllocateRequest(ask);

    UserGroupInformation ugi =
      UserGroupInformation.createRemoteUser(appAttemptId.toString());
    Token<AMRMTokenIdentifier> token = rm.getRMContext().getRMApps()
      .get(appAttemptId.getApplicationId())
      .getRMAppAttempt(appAttemptId).getAMRMToken();
    ugi.addTokenIdentifier(token.decodeIdentifier());
    AllocateResponse response = ugi.doAs(
      new PrivilegedExceptionAction<AllocateResponse>() {
        @Override
        public AllocateResponse run() throws Exception {
          return rm.getApplicationMasterService().allocate(request);
        }
      });
    if (response != null) {
      responseQueue.put(response);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  protected void processResponseQueue()
    throws InterruptedException, YarnException, IOException {
    // Check whether receive the am container
    if (!isAMContainerRunning) {
      if (!responseQueue.isEmpty()) {
        AllocateResponse response = responseQueue.take();
        if (response != null
          && !response.getAllocatedContainers().isEmpty()) {
          // Get AM container
          Container container = response.getAllocatedContainers().get(0);
          simulationContext.getNodeManagers().get(container.getNodeId())
            .addNewContainer(container, -1L);
          // Start AM container
          amContainer = container;
          LOG.debug(MessageFormat.format("Application {0} starts its " +
            "AM container ({1}).", appId, amContainer.getId()));
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
              LOG.debug(MessageFormat.format("Application {0} has one" +
                "mapper finished ({1}).", appId, containerId));
              assignedMaps.remove(containerId);
              mapFinished++;
              finishedContainers++;
            } else if (assignedReduces.containsKey(containerId)) {
              LOG.debug(MessageFormat.format("Application {0} has one" +
                "reducer finished ({1}).", appId, containerId));
              assignedReduces.remove(containerId);
              reduceFinished++;
              finishedContainers++;
            } else {
              // am container released event
              isFinished = true;
              LOG.info(MessageFormat.format("Application {0} goes to " +
                "cleanUp.", appId));
            }
          } else {
            // container to be killed
            if (assignedMaps.containsKey(containerId)) {
              LOG.debug(MessageFormat.format("Application {0} has one " +
                "mapper killed ({1}).", appId, containerId));
              pendingFailedMaps.add(assignedMaps.remove(containerId));
            } else if (assignedReduces.containsKey(containerId)) {
              LOG.debug(MessageFormat.format("Application {0} has one " +
                "reducer killed ({1}).", appId, containerId));
              pendingFailedReduces.add(assignedReduces.remove(containerId));
            } else {
              LOG.info(MessageFormat.format("Application {0}'s AM is " +
                "going to be killed. Restarting...", appId));
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
        simulationContext.getNodeManagers().get(amContainer.getNodeId())
          .cleanupContainer(amContainer.getId());
        isAMContainerRunning = false;
        LOG.debug(MessageFormat.format("Application {0} sends out event " +
          "to clean up its AM container.", appId));
        isFinished = true;
        simulationContext.getJobCompletionHandler().handle(oldAppId);
        break;
      }

      // check allocated containers
      for (Container container : response.getAllocatedContainers()) {
        if (!scheduledMaps.isEmpty()) {
          SimulatedContainer cs = scheduledMaps.remove();
          LOG.debug(MessageFormat.format("Application {0} starts a " +
            "launch a mapper ({1}).", appId, container.getId()));
          assignedMaps.put(container.getId(), cs);
          simulationContext.getNodeManagers().get(container.getNodeId())
            .addNewContainer(container, cs.getLifeTime());
        } else if (!this.scheduledReduces.isEmpty()) {
          SimulatedContainer cs = scheduledReduces.remove();
          LOG.debug(MessageFormat.format("Application {0} starts a " +
            "launch a reducer ({1}).", appId, container.getId()));
          assignedReduces.put(container.getId(), cs);
          simulationContext.getNodeManagers().get(container.getNodeId())
            .addNewContainer(container, cs.getLifeTime());
        }
      }
    }
  }

  /**
   * restart running because of the am container killed
   */
  private void restart()
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
    pendingReduces.addAll(pendingReduces);
    isAMContainerRunning = false;
    amContainer = null;
    // resent am container request
    requestAMContainer();
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
          LOG.debug(MessageFormat.format("Application {0} sends out " +
            "request for {1} mappers.", appId, pendingMaps.size()));
          scheduledMaps.addAll(pendingMaps);
          pendingMaps.clear();
        } else if (!pendingFailedMaps.isEmpty() && scheduledMaps.isEmpty()) {
          ask = packageRequests(pendingFailedMaps, PRIORITY_MAP);
          LOG.debug(MessageFormat.format("Application {0} sends out " +
              "requests for {1} failed mappers.", appId,
            pendingFailedMaps.size()));
          scheduledMaps.addAll(pendingFailedMaps);
          pendingFailedMaps.clear();
        }
      } else if (reduceFinished != reduceTotal) {
        // reduce phase
        if (!pendingReduces.isEmpty()) {
          ask = packageRequests(pendingReduces, PRIORITY_REDUCE);
          LOG.debug(MessageFormat.format("Application {0} sends out " +
            "requests for {1} reducers.", appId, pendingReduces.size()));
          scheduledReduces.addAll(pendingReduces);
          pendingReduces.clear();
        } else if (!pendingFailedReduces.isEmpty()
          && scheduledReduces.isEmpty()) {
          ask = packageRequests(pendingFailedReduces, PRIORITY_REDUCE);
          LOG.debug(MessageFormat.format("Application {0} sends out " +
              "request for {1} failed reducers.", appId,
            pendingFailedReduces.size()));
          scheduledReduces.addAll(pendingFailedReduces);
          pendingFailedReduces.clear();
        }
      }
    }
    if (ask == null) {
      ask = new ArrayList<ResourceRequest>();
    }

    final AllocateRequest request = createAllocateRequest(ask);
    if (totalContainers == 0) {
      request.setProgress(1.0f);
    } else {
      request.setProgress((float) finishedContainers / totalContainers);
    }

    UserGroupInformation ugi =
      UserGroupInformation.createRemoteUser(appAttemptId.toString());
    Token<AMRMTokenIdentifier> token = rm.getRMContext().getRMApps()
      .get(appAttemptId.getApplicationId())
      .getRMAppAttempt(appAttemptId).getAMRMToken();
    ugi.addTokenIdentifier(token.decodeIdentifier());
    AllocateResponse response = ugi.doAs(
      new PrivilegedExceptionAction<AllocateResponse>() {
        @Override
        public AllocateResponse run() throws Exception {
          return rm.getApplicationMasterService().allocate(request);
        }
      });
    if (response != null) {
      responseQueue.put(response);
    }
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
