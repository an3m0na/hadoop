package org.apache.hadoop.tools.posum.simulation.core.appmaster;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.tools.posum.simulation.core.SimulationContext;
import org.apache.hadoop.tools.posum.simulation.core.daemon.WorkerDaemon;
import org.apache.hadoop.tools.posum.simulation.core.dispatcher.ApplicationEvent;
import org.apache.hadoop.tools.posum.simulation.core.nodemanager.SimulatedContainer;
import org.apache.hadoop.tools.posum.simulation.util.AMCore;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.hadoop.tools.posum.common.util.cluster.ClusterUtils.createResourceRequest;
import static org.apache.hadoop.tools.posum.simulation.core.dispatcher.ApplicationEventType.APPLICATION_SUBMITTED;
import static org.apache.hadoop.yarn.api.records.ResourceRequest.ANY;

@Private
@Unstable
public abstract class AMDaemon extends WorkerDaemon {
  private String oldAppId;
  protected List<SimulatedContainer> initialContainers;
  final BlockingQueue<AllocateResponse> responseQueue;
  String amType;
  // progress
  int totalContainers;
  int finishedContainers;
  AMCore core;
  private volatile boolean registered = false;

  protected final Logger LOG = Logger.getLogger(AMDaemon.class);
  protected String hostName;
  private Map<Priority, Map<String, ResourceRequest>> requests = new HashMap<>();
  private boolean resendRequests = false;
  private List<ContainerId> releaseList = new LinkedList<>();

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
    simulationContext.getDispatcher().getEventHandler().handle(new ApplicationEvent(APPLICATION_SUBMITTED, oldAppId, core.getAppId()));

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
    // send out request
    sendContainerRequests();

    // process responses in the queue
    processResponseQueue();
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

  protected void sendContainerRequests() throws Exception {
    List<ResourceRequest> ask = new ArrayList<>();
    if (resendRequests) {
      for (Map<String, ResourceRequest> requestsByResource : requests.values()) {
        for (ResourceRequest request : requestsByResource.values()) {
          // create a copy so that it will not be modified by scheduler
          ask.add(ResourceRequest.newInstance(request.getPriority(), request.getResourceName(), request.getCapability(), request.getNumContainers()));
        }
      }
      LOG.trace(MessageFormat.format("Sim={0} T={1}: Application {2} sends out allocate request for {3}", simulationContext.getSchedulerClass().getSimpleName(), simulationContext.getCurrentTime(), core.getAppId(), ask));
    }

    final AllocateRequest request = core.createAllocateRequest(ask);
    request.setProgress(totalContainers == 0 ? 1.0f : (float) finishedContainers / totalContainers);
    request.setReleaseList(releaseList);

    AllocateResponse response = core.sendAllocateRequest(request);
    if (response != null) {
      responseQueue.put(response);
    }
    resendRequests = false;
  }

  protected void createRequests(List<SimulatedContainer> csList, Priority priority) {
    if (csList.isEmpty())
      return;
    for (SimulatedContainer container : csList)
      incContainerRequest(container.getResource(), getAllResourceNames(container), priority);
    resendRequests = true;
  }

  private void incContainerRequest(Resource resource, List<String> resourceNames, Priority priority) {
    Map<String, ResourceRequest> requestMap = requests.get(priority);
    if (requestMap == null) {
      requestMap = new HashMap<>();
      requests.put(priority, requestMap);
    }
    for (String name : resourceNames) {
      ResourceRequest request = requestMap.get(name);
      if (request != null) {
        request.setNumContainers(request.getNumContainers() + 1);
      } else {
        requestMap.put(name, createResourceRequest(priority, resource, name, 1));
      }
    }
  }

  protected void removeRequests(SimulatedContainer simulatedContainer, Priority priority) {
    List<String> resourceNames = getAllResourceNames(simulatedContainer);
    for (String resourceName : resourceNames) {
      decContainerRequest(priority, resourceName);
    }
    resendRequests = true;
  }

  private void decContainerRequest(Priority priority, String resourceName) {
    Map<String, ResourceRequest> requestMap = requests.get(priority);
    ResourceRequest request = requestMap.get(resourceName);
    request.setNumContainers(request.getNumContainers() - 1);
    if (!resourceName.equals(ANY) && request.getNumContainers() == 0)
      requestMap.remove(resourceName);
  }

  private List<String> getAllResourceNames(SimulatedContainer container) {
    List<String> locations = new ArrayList<>();
    if (simulationContext.isOnlineSimulation() && container.getHostName() != null)
      locations.add(container.getHostName());
    else
      locations.addAll(container.getPreferredLocations());
    locations.addAll(simulationContext.getTopologyProvider().getRacks(locations));
    locations.add(ANY);
    return locations;
  }

  protected void releaseContainer(Container container) {
    releaseList.add(container.getId());
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
