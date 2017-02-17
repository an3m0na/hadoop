package org.apache.hadoop.tools.posum.simulation.core;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.TaskProfile;
import org.apache.hadoop.tools.posum.simulation.core.daemon.DaemonRunner;
import org.apache.hadoop.tools.posum.simulation.core.daemon.appmaster.AMSimulator;
import org.apache.hadoop.tools.posum.simulation.core.daemon.appmaster.MRAMSimulator;
import org.apache.hadoop.tools.posum.simulation.core.daemon.nodemanager.ContainerSimulator;
import org.apache.hadoop.tools.posum.simulation.core.daemon.nodemanager.NMSimulator;
import org.apache.hadoop.tools.posum.simulation.core.daemon.scheduler.ResourceSchedulerWrapper;
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
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
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
import static org.apache.hadoop.tools.posum.simulation.core.SimulationConfiguration.RUNNER_POOL_SIZE;
import static org.apache.hadoop.tools.posum.simulation.core.SimulationConfiguration.RUNNER_POOL_SIZE_DEFAULT;

public class DaemonInitializer {

  // logger
  private final static Logger LOG = Logger.getLogger(DaemonInitializer.class);

  // RM, Runner
  private ResourceManager rm;
  private static DaemonRunner runner;
  private Configuration conf;
  private Map<String, Integer> queueAppNumMap;

  // NM simulator
  private HashMap<NodeId, NMSimulator> nmMap;
  private int nmMemoryMB, nmVCores;

  // AM simulator
  private Map<String, AMSimulator> amMap;
  private static int remainingApps = 0;

  // other simulation information
  private int numNMs, numAMs;
  private long maxRuntime;

  private String schedulerClass;
  private Set<String> nodeSet;
  private List<JobProfile> jobs;
  private Map<String, List<TaskProfile>> tasks;
  private static CountDownLatch amCounter;
  private Map<String, String> hostnameMap;
  private Map<String, String> racks;
  private static final String HOST_BASE = "192.168.1.";

  public DaemonInitializer(Configuration conf,
                           String schedulerClass,
                           Set<String> nodeSet,
                           Map<String, String> racks,
                           List<JobProfile> jobs,
                           Map<String, List<TaskProfile>> tasks
  ) throws IOException, ClassNotFoundException {

    this.schedulerClass = schedulerClass;
    this.nodeSet = nodeSet;
    this.jobs = jobs;
    this.tasks = tasks;
    this.conf = conf;
    this.racks = racks;

    // runner
    int poolSize = conf.getInt(RUNNER_POOL_SIZE, RUNNER_POOL_SIZE_DEFAULT);
    runner = new DaemonRunner(poolSize);
  }

  public void start() throws Exception {
    // start resource manager
    startRM();
    // start node managers
    queueNMs();
    // start application masters
    queueAMs();

    // blocked until all nodes RUNNING
    waitForNodesRunning();

    runner.start();

    runner.await(amCounter);

    LOG.info("DaemonInitializer finished.");
  }

  private void startRM() throws IOException, ClassNotFoundException {
    Configuration rmConf = new YarnConfiguration();
    rmConf.set(RM_SCHEDULER, schedulerClass);
    rmConf.set(YarnConfiguration.RM_SCHEDULER, ResourceSchedulerWrapper.class.getName());
    rm = new ResourceManager();
    rm.init(rmConf);
    rm.start();
  }

  private void queueNMs() throws YarnException, IOException {
    // nm configuration
    nmMemoryMB = conf.getInt(NM_MEMORY_MB, NM_MEMORY_MB_DEFAULT);
    nmVCores = conf.getInt(NM_VCORES, NM_VCORES_DEFAULT);
    int heartbeatInterval = conf.getInt(NM_HEARTBEAT_INTERVAL_MS, NM_HEARTBEAT_INTERVAL_MS_DEFAULT);

    // create NM simulators

    nmMap = new HashMap<>(nodeSet.size());
    hostnameMap = new HashMap<>(nodeSet.size());
    Random random = new Random();
    for (String oldHostname : nodeSet) {
      // randomize the start time from -heartbeatInterval to zero, in order to start NMs before AMs
      NMSimulator nm = new NMSimulator(runner);
      nm.init(racks.get(oldHostname), registerNode(oldHostname), nmMemoryMB, nmVCores, -random.nextInt(heartbeatInterval), heartbeatInterval, rm);
      nmMap.put(nm.getNode().getNodeID(), nm);
      runner.schedule(nm);
    }
  }

  private String registerNode(String hostName) {
    String newHostname = HOST_BASE + numNMs++;
    hostnameMap.put(hostName, newHostname);
    return newHostname;
  }

  private void waitForNodesRunning() throws InterruptedException {
    long startTimeMS = System.currentTimeMillis();
    while (true) {
      int numRunningNodes = 0;
      for (RMNode node : rm.getRMContext().getRMNodes().values()) {
        if (node.getState() == NodeState.RUNNING) {
          numRunningNodes++;
        }
      }
      if (numRunningNodes == numNMs) {
        break;
      }
      LOG.info(MessageFormat.format("DaemonInitializer is waiting for all " +
          "nodes RUNNING. {0} of {1} NMs initialized.",
        numRunningNodes, numNMs));
      Thread.sleep(100);
    }
    LOG.info(MessageFormat.format("DaemonInitializer takes {0} ms to launch all nodes.",
      (System.currentTimeMillis() - startTimeMS)));
  }

  private void queueAMs() throws YarnException, IOException {
    // application/container configuration
    int heartbeatInterval = conf.getInt(AM_HEARTBEAT_INTERVAL_MS, AM_HEARTBEAT_INTERVAL_MS_DEFAULT);
    int containerMemoryMB = conf.getInt(CONTAINER_MEMORY_MB, CONTAINER_MEMORY_MB_DEFAULT);
    int containerVCores = conf.getInt(CONTAINER_VCORES, CONTAINER_VCORES_DEFAULT);
    Resource containerResource =
      BuilderUtils.newResource(containerMemoryMB, containerVCores);

    startAMForJobs(containerResource, heartbeatInterval);
    numAMs = amMap.size();
    amCounter = new CountDownLatch(numAMs);
  }

  private void startAMForJobs(Resource containerResource,
                              int heartbeatInterval) throws IOException {
    long baselineTime = 0;
    queueAppNumMap = new HashMap<>();
    amMap = new HashMap<>(jobs.size());

    for (JobProfile job : jobs) {
      // load job information
      long jobStartTime = job.getStartTime();
      long jobFinishTime = job.getFinishTime();
      if (baselineTime == 0)
        baselineTime = jobStartTime;
      jobStartTime -= baselineTime;
      jobFinishTime -= baselineTime;
      if (jobStartTime < 0) {
        LOG.warn("Warning: reset job " + job.getId() + " start time to 0.");
        jobFinishTime = jobFinishTime - jobStartTime;
        jobStartTime = 0;
      }

      String user = job.getUser();
      String queue = job.getQueue();
      String appId = job.getAppId();
      int queueSize = queueAppNumMap.containsKey(queue) ? queueAppNumMap.get(queue) : 0;
      queueSize++;
      queueAppNumMap.put(queue, queueSize);
      List<ContainerSimulator> containerList = new ArrayList<>();
      for (TaskProfile task : tasks.get(job.getId())) {
        String hostname = hostnameMap.get(task.getHttpAddress());
        String rack = racks.get(task.getHttpAddress());
        long taskStart = task.getStartTime();
        long taskFinish = task.getFinishTime();
        long lifeTime = taskFinish - taskStart;
        int priority = 0;
        String type = task.getType() == MAP ? "map" : "reduce";
        containerList.add(new ContainerSimulator(runner, containerResource,
          lifeTime, rack, hostname, priority, type));
      }

      // create a new AM
      AMSimulator amSim = new MRAMSimulator(runner);
      amSim.init(heartbeatInterval, containerList, rm, this, jobStartTime, user, queue, false, appId);
      runner.schedule(amSim);
      maxRuntime = Math.max(maxRuntime, jobFinishTime);
      amMap.put(appId, amSim);
    }
  }

  public HashMap<NodeId, NMSimulator> getNmMap() {
    return nmMap;
  }

  public static DaemonRunner getRunner() {
    return runner;
  }

  public static void decreaseRemainingApps() {
    amCounter.countDown();
  }
}
