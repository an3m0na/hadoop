package org.apache.hadoop.tools.posum.simulation.core.daemons;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.TaskProfile;
import org.apache.hadoop.tools.posum.common.util.PosumConfiguration;
import org.apache.hadoop.tools.posum.simulation.core.daemons.appmaster.AMSimulator;
import org.apache.hadoop.tools.posum.simulation.core.daemons.appmaster.MRAMSimulator;
import org.apache.hadoop.tools.posum.simulation.core.daemons.nodemanager.NMSimulator;
import org.apache.hadoop.tools.posum.simulation.core.daemons.scheduler.ContainerSimulator;
import org.apache.hadoop.tools.posum.simulation.core.daemons.scheduler.ResourceSchedulerWrapper;
import org.apache.hadoop.tools.posum.simulation.core.daemons.scheduler.TaskRunner;
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

public class DaemonRunner {

  // logger
  private final static Logger LOG = Logger.getLogger(DaemonRunner.class);

  // RM, Runner
  private ResourceManager rm;
  private static TaskRunner runner = new TaskRunner();
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

  public static class SimulationConfiguration {
    public static final String PREFIX = PosumConfiguration.SIMULATOR_PREFIX;
    // runner
    public static final String RUNNER_PREFIX = PREFIX + "runner.";
    public static final String RUNNER_POOL_SIZE = RUNNER_PREFIX + "pool.size";
    public static final int RUNNER_POOL_SIZE_DEFAULT = 10;
    // scheduler
    public static final String SCHEDULER_PREFIX = PREFIX + "scheduler.";
    public static final String RM_SCHEDULER = SCHEDULER_PREFIX + "class";
    // nm
    public static final String NM_PREFIX = PREFIX + "nm.";
    public static final String NM_MEMORY_MB = NM_PREFIX + "memory.mb";
    public static final int NM_MEMORY_MB_DEFAULT = 10240;
    public static final String NM_VCORES = NM_PREFIX + "vcores";
    public static final int NM_VCORES_DEFAULT = 10;
    public static final String NM_HEARTBEAT_INTERVAL_MS = NM_PREFIX
      + "heartbeat.interval.ms";
    public static final int NM_HEARTBEAT_INTERVAL_MS_DEFAULT = 1000;
    // am
    public static final String AM_PREFIX = PREFIX + "am.";
    public static final String AM_HEARTBEAT_INTERVAL_MS = AM_PREFIX
      + "heartbeat.interval.ms";
    public static final int AM_HEARTBEAT_INTERVAL_MS_DEFAULT = 1000;
    // container
    public static final String CONTAINER_PREFIX = PREFIX + "container.";
    public static final String CONTAINER_MEMORY_MB = CONTAINER_PREFIX
      + "memory.mb";
    public static final int CONTAINER_MEMORY_MB_DEFAULT = 1024;
    public static final String CONTAINER_VCORES = CONTAINER_PREFIX + "vcores";
    public static final int CONTAINER_VCORES_DEFAULT = 1;
  }

  public DaemonRunner(Configuration conf,
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
    int poolSize = conf.getInt(SimulationConfiguration.RUNNER_POOL_SIZE,
      SimulationConfiguration.RUNNER_POOL_SIZE_DEFAULT);
    runner.setQueueSize(poolSize);
  }

  public void start() throws Exception {
    // start resource manager
    startRM();
    // start node managers
    startNM();

    runner.start();
    // blocked until all nodes RUNNING
    waitForNodesRunning();

    // start application masters
    startAM();

    runner.await(amCounter);

    LOG.info("DaemonRunner finished.");
  }

  private void startRM() throws IOException, ClassNotFoundException {
    Configuration rmConf = new YarnConfiguration();
    rmConf.set(SimulationConfiguration.RM_SCHEDULER, schedulerClass);
    rmConf.set(YarnConfiguration.RM_SCHEDULER, ResourceSchedulerWrapper.class.getName());
    rm = new ResourceManager();
    rm.init(rmConf);
    rm.start();
  }

  private void startNM() throws YarnException, IOException {
    // nm configuration
    nmMemoryMB = conf.getInt(SimulationConfiguration.NM_MEMORY_MB,
      SimulationConfiguration.NM_MEMORY_MB_DEFAULT);
    nmVCores = conf.getInt(SimulationConfiguration.NM_VCORES,
      SimulationConfiguration.NM_VCORES_DEFAULT);
    int heartbeatInterval = conf.getInt(
      SimulationConfiguration.NM_HEARTBEAT_INTERVAL_MS,
      SimulationConfiguration.NM_HEARTBEAT_INTERVAL_MS_DEFAULT);

    // create NM simulators

    nmMap = new HashMap<>(nodeSet.size());
    hostnameMap = new HashMap<>(nodeSet.size());
    Random random = new Random();
    for (String oldHostname : nodeSet) {
      // we randomize the heartbeat start time from zero to 1 interval
      NMSimulator nm = new NMSimulator();
      String newHostname = registerNode(oldHostname);
      nm.init(newHostname, nmMemoryMB, nmVCores, random.nextInt(heartbeatInterval), heartbeatInterval, rm);
      nmMap.put(nm.getNode().getNodeID(), nm);
      runner.schedule(nm);
    }
  }

  private String registerNode(String hostName) {
    String rack = racks.get(hostName);
    String newHostname = "/" + rack + "/" + HOST_BASE + numNMs++;
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
      LOG.info(MessageFormat.format("DaemonRunner is waiting for all " +
          "nodes RUNNING. {0} of {1} NMs initialized.",
        numRunningNodes, numNMs));
      Thread.sleep(100);
    }
    LOG.info(MessageFormat.format("DaemonRunner takes {0} ms to launch all nodes.",
      (System.currentTimeMillis() - startTimeMS)));
  }

  @SuppressWarnings("unchecked")
  private void startAM() throws YarnException, IOException {
    // application/container configuration
    int heartbeatInterval = conf.getInt(
      SimulationConfiguration.AM_HEARTBEAT_INTERVAL_MS,
      SimulationConfiguration.AM_HEARTBEAT_INTERVAL_MS_DEFAULT);
    int containerMemoryMB = conf.getInt(SimulationConfiguration.CONTAINER_MEMORY_MB,
      SimulationConfiguration.CONTAINER_MEMORY_MB_DEFAULT);
    int containerVCores = conf.getInt(SimulationConfiguration.CONTAINER_VCORES,
      SimulationConfiguration.CONTAINER_VCORES_DEFAULT);
    Resource containerResource =
      BuilderUtils.newResource(containerMemoryMB, containerVCores);

    startAMForJobs(containerResource, heartbeatInterval);
    numAMs = amMap.size();
    amCounter = new CountDownLatch(numAMs);
  }

  @SuppressWarnings("unchecked")
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
        String hostname = resolveHostname(task.getHttpAddress());
        long taskStart = task.getStartTime();
        long taskFinish = task.getFinishTime();
        long lifeTime = taskFinish - taskStart;
        int priority = 0;
        String type = task.getType() == MAP ? "map" : "reduce";
        containerList.add(new ContainerSimulator(containerResource,
          lifeTime, hostname, priority, type));
      }

      // create a new AM
      AMSimulator amSim = new MRAMSimulator();
      amSim.init(heartbeatInterval, containerList, rm,
        this, jobStartTime, jobFinishTime, user, queue,
        false, appId);
      runner.schedule(amSim);
      maxRuntime = Math.max(maxRuntime, jobFinishTime);
      amMap.put(appId, amSim);
    }
  }

  private String resolveHostname(String httpAddress) {
    String rack = racks.get(httpAddress);
    return "/" + rack + "/" + hostnameMap.get(httpAddress);
  }

  public HashMap<NodeId, NMSimulator> getNmMap() {
    return nmMap;
  }

  public static TaskRunner getRunner() {
    return runner;
  }

  public static void decreaseRemainingApps() {
    amCounter.countDown();
  }

  public static long getCurrentTime(){
    return System.currentTimeMillis();
  }
}
