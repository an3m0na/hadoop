package org.apache.hadoop.tools.posum.simulation.core.appmaster;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.tools.posum.common.util.cluster.AMCore;
import org.apache.hadoop.tools.posum.simulation.core.SimulationContext;
import org.apache.hadoop.tools.posum.simulation.core.daemon.WorkerDaemon;
import org.apache.hadoop.tools.posum.simulation.core.dispatcher.ApplicationEvent;
import org.apache.hadoop.tools.posum.simulation.core.nodemanager.SimulatedContainer;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.hadoop.tools.posum.common.util.Utils.createResourceRequest;
import static org.apache.hadoop.tools.posum.simulation.core.dispatcher.ApplicationEventType.APPLICATION_SUBMITTED;

@Private
@Unstable
public abstract class AMDaemon extends WorkerDaemon {
  private String oldAppId;
  List<SimulatedContainer> initialContainers;
  final BlockingQueue<AllocateResponse> responseQueue;
  String amType;
  // progress
  int totalContainers;
  int finishedContainers;
  AMCore core;
  private volatile boolean registered = false;

  protected final Logger LOG = Logger.getLogger(AMDaemon.class);
  String hostName;

  public AMDaemon(SimulationContext simulationContext) {
    super(simulationContext);
    this.responseQueue = new LinkedBlockingQueue<>();
  }

  public void init(int heartbeatInterval, List<SimulatedContainer> containerList, ResourceManager rm,
                   long traceStartTime, String user, String queue, String oldAppId, String hostName) {
    this.hostName = hostName;
    super.init(traceStartTime, heartbeatInterval);
    this.core = new AMCore(rm, user, queue);
    this.oldAppId = oldAppId;
    this.initialContainers = containerList;
  }

  /**
   * register with RM
   */
  @Override
  public void doFirstStep() throws Exception {
    LOG.trace(MessageFormat.format("Sim={0} T={1}: Submitting app for {2}", simulationContext.getSchedulerClass().getSimpleName(), oldAppId));
    core.submit();
    simulationContext.getDispatcher().getEventHandler()
      .handle(new ApplicationEvent(APPLICATION_SUBMITTED, oldAppId, core.getAppId()));

    LOG.trace(MessageFormat.format("Sim={0} T={1}: Registering a new application {2}", simulationContext.getSchedulerClass().getSimpleName(), simulationContext.getCurrentTime(), core.getAppId()));
    core.registerWithRM();
    LOG.trace(MessageFormat.format("Sim={0} T={1}: Application {2} is registered", simulationContext.getSchedulerClass().getSimpleName(), simulationContext.getCurrentTime(), core.getAppId()));

    requestAMContainer();

    registered = true;
  }

  private void requestAMContainer() throws YarnException, IOException, InterruptedException {
    AllocateResponse response;
    if (simulationContext.isOnlineSimulation() && hostName != null) {
      LOG.trace(MessageFormat.format("Sim={0} T={1}: Application {2} sends out allocate request for its AM on {3}", simulationContext.getSchedulerClass().getSimpleName(), simulationContext.getCurrentTime(), core.getAppId(), hostName));
      response = core.requestContainerOnNode(hostName, simulationContext.getTopologyProvider().getRack(hostName));
    } else {
      LOG.trace(MessageFormat.format("Sim={0} T={1}: Application {2} sends out allocate request for its AM", simulationContext.getSchedulerClass().getSimpleName(), simulationContext.getCurrentTime(), core.getAppId()));
      response = core.requestContainer();
    }
    if (response != null) {
      responseQueue.put(response);
    }
  }

  @Override
  public void doStep() throws Exception {
    // process responses in the queue
    processResponseQueue();

    // send out request
    sendContainerRequest();
  }

  @Override
  public void cleanUp() throws Exception {
    LOG.trace(MessageFormat.format("Sim={0} T={1}: Application {2} is shutting down.", simulationContext.getSchedulerClass().getSimpleName(), simulationContext.getCurrentTime(), core.getAppId()));
    core.unregister();
  }

  /**
   * restart running because of the am container killed
   */
  void restart() throws YarnException, IOException, InterruptedException {
    // resent am container request
    requestAMContainer();
  }

  protected abstract void processResponseQueue() throws Exception;

  protected abstract void sendContainerRequest() throws Exception;

  protected List<ResourceRequest> packageRequests(
    List<SimulatedContainer> csList, int priority) {
    // create requests
    Map<String, ResourceRequest> rackLocalRequestMap = new HashMap<>();
    Map<String, ResourceRequest> nodeLocalRequestMap = new HashMap<>();
    ResourceRequest anyRequest = null;
    for (SimulatedContainer cs : csList) {
      List<String> hosts = cs.getHostName() != null ? Collections.singletonList(cs.getHostName()) : cs.getPreferredLocations();
      List<String> racks = simulationContext.getTopologyProvider().getRacks(hosts);
      // check rack local
      addContainers(rackLocalRequestMap, cs.getResource(), racks, priority);
      // check node local
      addContainers(nodeLocalRequestMap, cs.getResource(), hosts, priority);
      // any
      if (anyRequest == null) {
        anyRequest = createResourceRequest(priority, cs.getResource(), ResourceRequest.ANY, 1);
      } else {
        anyRequest.setNumContainers(anyRequest.getNumContainers() + 1);
      }
    }
    List<ResourceRequest> ask = new ArrayList<>();
    ask.addAll(nodeLocalRequestMap.values());
    ask.addAll(rackLocalRequestMap.values());
    if (anyRequest != null) {
      ask.add(anyRequest);
    }
    return ask;
  }

  private void addContainers(Map<String, ResourceRequest> requestMap, Resource resource, List<String> locations, int priority) {
    for (String rack : locations) {
      ResourceRequest request = requestMap.get(rack);
      if (request != null) {
        request.setNumContainers(request.getNumContainers() + 1);
      } else {
        requestMap.put(rack, createResourceRequest(priority, resource, rack, 1));
      }
    }
  }

  public ApplicationId getAppId() {
    return core.getAppId();
  }

  public List<SimulatedContainer> getContainers() {
    return initialContainers;
  }

  public boolean isRegistered() {
    return registered;
  }

  public String getHostName() {
    return hostName;
  }
}
