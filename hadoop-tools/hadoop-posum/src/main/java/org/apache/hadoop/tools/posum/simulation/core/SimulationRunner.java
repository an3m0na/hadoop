package org.apache.hadoop.tools.posum.simulation.core;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.TaskProfile;
import org.apache.hadoop.tools.posum.simulation.core.appmaster.AMSimulator;
import org.apache.hadoop.tools.posum.simulation.core.appmaster.MRAMSimulator;
import org.apache.hadoop.tools.posum.simulation.core.daemon.DaemonPool;
import org.apache.hadoop.tools.posum.simulation.core.nodemanager.ContainerSimulator;
import org.apache.hadoop.tools.posum.simulation.core.nodemanager.NMSimulator;
import org.apache.hadoop.tools.posum.simulation.core.resourcemanager.ResourceManagerWrapper;
import org.apache.hadoop.tools.posum.simulation.core.resourcemanager.ResourceSchedulerWrapper;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import static org.apache.hadoop.mapreduce.v2.api.records.TaskType.MAP;
import static org.apache.hadoop.tools.posum.simulation.core.SimulationConfiguration.AM_HEARTBEAT_INTERVAL_MS;
import static org.apache.hadoop.tools.posum.simulation.core.SimulationConfiguration.AM_HEARTBEAT_INTERVAL_MS_DEFAULT;
import static org.apache.hadoop.tools.posum.simulation.core.SimulationConfiguration.CONTAINER_MEMORY_MB;
import static org.apache.hadoop.tools.posum.simulation.core.SimulationConfiguration.CONTAINER_MEMORY_MB_DEFAULT;
import static org.apache.hadoop.tools.posum.simulation.core.SimulationConfiguration.CONTAINER_VCORES;
import static org.apache.hadoop.tools.posum.simulation.core.SimulationConfiguration.CONTAINER_VCORES_DEFAULT;
import static org.apache.hadoop.tools.posum.simulation.core.SimulationConfiguration.NM_HEARTBEAT_INTERVAL_MS;
import static org.apache.hadoop.tools.posum.simulation.core.SimulationConfiguration.NM_HEARTBEAT_INTERVAL_MS_DEFAULT;
import static org.apache.hadoop.tools.posum.simulation.core.SimulationConfiguration.NM_MEMORY_MB;
import static org.apache.hadoop.tools.posum.simulation.core.SimulationConfiguration.NM_MEMORY_MB_DEFAULT;
import static org.apache.hadoop.tools.posum.simulation.core.SimulationConfiguration.NM_VCORES;
import static org.apache.hadoop.tools.posum.simulation.core.SimulationConfiguration.NM_VCORES_DEFAULT;
import static org.apache.hadoop.tools.posum.simulation.core.SimulationConfiguration.RM_SCHEDULER;

public class SimulationRunner {
  private final static Logger LOG = Logger.getLogger(SimulationRunner.class);
  private static final String HOST_BASE = "192.168.1."; // needed because hostnames need to be resolvable

  private ResourceManager rm;
  private static DaemonPool daemonPool;
  private SimulationContext context;
  private Configuration conf;
  private Map<String, String> simulationHostNames;


  public SimulationRunner(SimulationContext context) throws IOException, ClassNotFoundException {
    this.context = context;
    this.conf = context.getConf();
    daemonPool = new DaemonPool(context);
  }

  public void start() throws Exception {
    startRM();
    queueNMs();
    queueAMs();

    // blocked until all NMs are RUNNING
    waitForNodesRunning();

    daemonPool.start();
    context.getRemainingJobsCounter().await();
    context.setEndTime(context.getCurrentTime());
    daemonPool.shutDown();

    LOG.info("SimulationRunner finished.");
  }

  private void startRM() throws IOException, ClassNotFoundException {
    Configuration rmConf = new YarnConfiguration();
    rmConf.set(RM_SCHEDULER, context.getSchedulerClass());
    rmConf.set(YarnConfiguration.RM_SCHEDULER, ResourceSchedulerWrapper.class.getName());
    rm = new ResourceManagerWrapper(context);
    rm.init(rmConf);
    rm.start();
  }

  private void queueNMs() throws YarnException, IOException {
    // nm configuration
    int nmMemoryMB = conf.getInt(NM_MEMORY_MB, NM_MEMORY_MB_DEFAULT);
    int nmVCores = conf.getInt(NM_VCORES, NM_VCORES_DEFAULT);
    int heartbeatInterval = conf.getInt(NM_HEARTBEAT_INTERVAL_MS, NM_HEARTBEAT_INTERVAL_MS_DEFAULT);

    Map<NodeId, NMSimulator> nmMap = new HashMap<>(context.getTopology().size());
    simulationHostNames = new HashMap<>(context.getTopology().size());
    Random random = new Random();
    for (String oldHostname : context.getTopology().keySet()) {
      // randomize the start time from -heartbeatInterval to zero, in order to start NMs before AMs
      NMSimulator nm = new NMSimulator(context);
      nm.init(context.getTopology().get(oldHostname), assignNewHost(oldHostname), nmMemoryMB, nmVCores, -random.nextInt(heartbeatInterval), heartbeatInterval, rm);
      nmMap.put(nm.getNode().getNodeID(), nm);
      daemonPool.schedule(nm);
    }
    context.setNodeManagers(nmMap);
  }

  private String assignNewHost(String hostName) {
    String newHostname = HOST_BASE + simulationHostNames.size();
    simulationHostNames.put(hostName, newHostname);
    return newHostname;
  }

  private void waitForNodesRunning() throws InterruptedException {
    while (true) {
      int numRunningNodes = 0;
      for (RMNode node : rm.getRMContext().getRMNodes().values()) {
        if (node.getState() == NodeState.RUNNING) {
          numRunningNodes++;
        }
      }
      if (numRunningNodes == context.getNodeManagers().size()) {
        break;
      }
      Thread.sleep(100);
    }
  }

  private void queueAMs() throws YarnException, IOException {
    // application/container configuration
    int heartbeatInterval = conf.getInt(AM_HEARTBEAT_INTERVAL_MS, AM_HEARTBEAT_INTERVAL_MS_DEFAULT);
    int containerMemoryMB = conf.getInt(CONTAINER_MEMORY_MB, CONTAINER_MEMORY_MB_DEFAULT);
    int containerVCores = conf.getInt(CONTAINER_VCORES, CONTAINER_VCORES_DEFAULT);
    Resource containerResource =
      BuilderUtils.newResource(containerMemoryMB, containerVCores);

    long baselineTime = 0;
    Map<String, Integer> queueAppNumMap = new HashMap<>();
    Map<String, AMSimulator> amMap = new HashMap<>(context.getJobs().size());

    for (JobProfile job : context.getJobs()) {
      // load job information
      long jobStartTime = job.getStartTime();
      if (baselineTime == 0)
        baselineTime = jobStartTime;
      jobStartTime -= baselineTime;
      if (jobStartTime < 0) {
        LOG.warn("Warning: reset job " + job.getId() + " start time to 0.");
        jobStartTime = 0;
      }

      String user = job.getUser();
      String queue = job.getQueue();
      String appId = job.getAppId();
      int queueSize = queueAppNumMap.containsKey(queue) ? queueAppNumMap.get(queue) : 0;
      queueSize++;
      queueAppNumMap.put(queue, queueSize);
      List<ContainerSimulator> containerList = new ArrayList<>();
      for (TaskProfile task : context.getTasks().get(job.getId())) {
        String hostname = simulationHostNames.get(task.getHttpAddress());
        String rack = context.getTopology().get(task.getHttpAddress());
        long taskStart = task.getStartTime();
        long taskFinish = task.getFinishTime();
        long lifeTime = taskFinish - taskStart;
        int priority = 0;
        String type = task.getType() == MAP ? "map" : "reduce";
        containerList.add(new ContainerSimulator(context, containerResource,
          lifeTime, rack, hostname, priority, type));
      }

      // create a new AM
      AMSimulator amSim = new MRAMSimulator(context);
      amSim.init(heartbeatInterval, containerList, rm, jobStartTime, user, queue, appId);
      daemonPool.schedule(amSim);
      amMap.put(appId, amSim);
    }

    context.setRemainingJobsCounter(new CountDownLatch(amMap.size()));
  }

}
