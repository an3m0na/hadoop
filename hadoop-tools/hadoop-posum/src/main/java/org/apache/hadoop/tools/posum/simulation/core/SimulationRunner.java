package org.apache.hadoop.tools.posum.simulation.core;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.client.data.Database;
import org.apache.hadoop.tools.posum.common.records.call.FindByIdCall;
import org.apache.hadoop.tools.posum.common.records.call.FindByQueryCall;
import org.apache.hadoop.tools.posum.common.records.call.IdsByQueryCall;
import org.apache.hadoop.tools.posum.common.records.call.query.QueryUtils;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobConfProxy;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.TaskProfile;
import org.apache.hadoop.tools.posum.simulation.core.appmaster.AMDaemon;
import org.apache.hadoop.tools.posum.simulation.core.appmaster.MRAMDaemon;
import org.apache.hadoop.tools.posum.simulation.core.daemon.DaemonPool;
import org.apache.hadoop.tools.posum.simulation.core.nodemanager.NMDaemon;
import org.apache.hadoop.tools.posum.simulation.core.nodemanager.SimulatedContainer;
import org.apache.hadoop.tools.posum.simulation.core.resourcemanager.ResourceManagerWrapper;
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
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static org.apache.hadoop.mapreduce.MRJobConfig.COMPLETED_MAPS_FOR_REDUCE_SLOWSTART;
import static org.apache.hadoop.mapreduce.v2.api.records.TaskType.MAP;
import static org.apache.hadoop.mapreduce.v2.app.rm.RMContainerAllocator.DEFAULT_COMPLETED_MAPS_PERCENT_FOR_REDUCE_SLOWSTART;
import static org.apache.hadoop.tools.posum.common.util.PosumConfiguration.AM_DAEMON_HEARTBEAT_INTERVAL_MS;
import static org.apache.hadoop.tools.posum.common.util.PosumConfiguration.AM_DAEMON_HEARTBEAT_INTERVAL_MS_DEFAULT;
import static org.apache.hadoop.tools.posum.common.util.PosumConfiguration.NM_DAEMON_HEARTBEAT_INTERVAL_MS;
import static org.apache.hadoop.tools.posum.common.util.PosumConfiguration.NM_DAEMON_HEARTBEAT_INTERVAL_MS_DEFAULT;
import static org.apache.hadoop.tools.posum.common.util.PosumConfiguration.NM_DAEMON_MEMORY_MB;
import static org.apache.hadoop.tools.posum.common.util.PosumConfiguration.NM_DAEMON_MEMORY_MB_DEFAULT;
import static org.apache.hadoop.tools.posum.common.util.PosumConfiguration.NM_DAEMON_VCORES;
import static org.apache.hadoop.tools.posum.common.util.PosumConfiguration.NM_DAEMON_VCORES_DEFAULT;
import static org.apache.hadoop.tools.posum.common.util.PosumConfiguration.SIMULATION_CONTAINER_MEMORY_MB;
import static org.apache.hadoop.tools.posum.common.util.PosumConfiguration.SIMULATION_CONTAINER_MEMORY_MB_DEFAULT;
import static org.apache.hadoop.tools.posum.common.util.PosumConfiguration.SIMULATION_CONTAINER_VCORES;
import static org.apache.hadoop.tools.posum.common.util.PosumConfiguration.SIMULATION_CONTAINER_VCORES_DEFAULT;
import static org.apache.hadoop.tools.posum.common.util.Utils.orZero;

public class SimulationRunner {
  private final static Logger LOG = Logger.getLogger(SimulationRunner.class);
  private static final String HOST_BASE = "192.168.1."; // needed because hostnames need to be resolvable
  private static final IdsByQueryCall GET_PENDING_JOBS = IdsByQueryCall.newInstance(DataEntityCollection.JOB, null, "startTime", false);
  private static final FindByIdCall GET_JOB = FindByIdCall.newInstance(DataEntityCollection.JOB, null);
  private static final FindByIdCall GET_CONF = FindByIdCall.newInstance(DataEntityCollection.JOB_CONF, null);
  private static final FindByQueryCall GET_TASKS = FindByQueryCall.newInstance(DataEntityCollection.TASK, null);

  private ResourceManager rm;
  private static DaemonPool daemonPool;
  private SimulationContext context;
  private Configuration conf;
  private Map<String, String> simulationHostNames;
  private Resource containerResource;
  private Map<NodeId, NMDaemon> nmMap;


  public SimulationRunner(SimulationContext context) throws IOException, ClassNotFoundException {
    this.context = context;
    conf = context.getConf();
    daemonPool = new DaemonPool(context);
    int containerMemoryMB = conf.getInt(SIMULATION_CONTAINER_MEMORY_MB, SIMULATION_CONTAINER_MEMORY_MB_DEFAULT);
    int containerVCores = conf.getInt(SIMULATION_CONTAINER_VCORES, SIMULATION_CONTAINER_VCORES_DEFAULT);
    containerResource = BuilderUtils.newResource(containerMemoryMB, containerVCores);
  }

  public void run() throws Exception {
    LOG.info("SimulationRunner starting for " + context.getSchedulerClass().getSimpleName());

    startRM();
    queueNMs();
    queueAMs();

    // blocked until all NMs are RUNNING
    waitForNodesRunning();

    daemonPool.start();
    context.getRemainingJobsCounter().await();
    context.setEndTime(context.getCurrentTime());
    daemonPool.shutDown();

    LOG.info("SimulationRunner finished for " + context.getSchedulerClass().getSimpleName());
  }

  private void startRM() throws IOException, ClassNotFoundException {
    rm = new ResourceManagerWrapper(context);
    rm.init(new YarnConfiguration());
    rm.start();
  }

  private void queueNMs() throws YarnException, IOException {
    // nm configuration
    int nmMemoryMB = conf.getInt(NM_DAEMON_MEMORY_MB, NM_DAEMON_MEMORY_MB_DEFAULT);
    int nmVCores = conf.getInt(NM_DAEMON_VCORES, NM_DAEMON_VCORES_DEFAULT);
    int heartbeatInterval = conf.getInt(NM_DAEMON_HEARTBEAT_INTERVAL_MS, NM_DAEMON_HEARTBEAT_INTERVAL_MS_DEFAULT);

    //FIXME: use a dynamic snapshot mechanism
    Set<String> activeNodes = context.getTopologyProvider().getActiveNodes();
    nmMap = new HashMap<>(activeNodes.size());
    simulationHostNames = new HashMap<>(activeNodes.size());
    Random random = new Random();
    for (String oldHostname : activeNodes) {
      // randomize the start time from -heartbeatInterval to zero, in order to start NMs before AMs
      NMDaemon nm = new NMDaemon(context);
      nm.init(context.getTopologyProvider().resolve(oldHostname),
        assignNewHost(oldHostname),
        oldHostname,
        nmMemoryMB,
        nmVCores,
        -random.nextInt(heartbeatInterval),
        heartbeatInterval, rm
      );
      nmMap.put(nm.getNode().getNodeID(), nm);
      daemonPool.schedule(nm);
    }
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
      if (numRunningNodes == nmMap.size()) {
        break;
      }
      Thread.sleep(100);
    }
  }

  private void queueAMs() throws YarnException, IOException {
    // application/container configuration
    int heartbeatInterval = conf.getInt(AM_DAEMON_HEARTBEAT_INTERVAL_MS, AM_DAEMON_HEARTBEAT_INTERVAL_MS_DEFAULT);


    Map<String, Integer> queueAppNumMap = new HashMap<>();

    Database sourceDb = context.getSourceDatabase();

    List<String> jobIds = sourceDb.execute(GET_PENDING_JOBS).getEntries();
    Map<String, AMDaemon> amMap = new HashMap<>(jobIds.size());

    long baselineTime = context.getStartTime();
    for (String jobId : jobIds) {
      // load job information
      GET_JOB.setId(jobId);
      JobProfile job = sourceDb.execute(GET_JOB).getEntity();
      long jobStartTime = orZero(job.getStartTime());
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

      GET_CONF.setId(job.getId());
      JobConfProxy jobConf = sourceDb.execute(GET_CONF).getEntity();
      float slowStartRatio = jobConf.getConf().getFloat(COMPLETED_MAPS_FOR_REDUCE_SLOWSTART,
        DEFAULT_COMPLETED_MAPS_PERCENT_FOR_REDUCE_SLOWSTART);

      // create a new AM
      MRAMDaemon amSim = new MRAMDaemon(context);
      amSim.init(heartbeatInterval, createContainers(jobId), rm, jobStartTime, user, queue, appId, slowStartRatio);
      daemonPool.schedule(amSim);
      amMap.put(appId, amSim);
    }

    context.setRemainingJobsCounter(new CountDownLatch(amMap.size()));
  }

  private List<SimulatedContainer> createContainers(String jobId) {
    List<SimulatedContainer> ret = new ArrayList<>();

    GET_TASKS.setQuery(QueryUtils.is("jobId", jobId));
    List<TaskProfile> jobTasks = context.getSourceDatabase().execute(GET_TASKS).getEntities();

    for (TaskProfile task : jobTasks) {
      Long taskStart = task.getStartTime();
      Long taskFinish = task.getFinishTime();
      Long lifeTime = taskStart != null && taskFinish != null? taskFinish - taskStart : null;
      int priority = 0;
      String type = task.getType() == MAP ? "map" : "reduce";
      ret.add(new SimulatedContainer(context, containerResource, lifeTime, priority, type, task.getId(), task.getSplitLocations()));
    }
    return ret;
  }

}
