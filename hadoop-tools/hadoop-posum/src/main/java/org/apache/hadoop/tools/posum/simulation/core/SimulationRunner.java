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
import org.apache.hadoop.tools.posum.common.util.conf.PosumConfiguration;
import org.apache.hadoop.tools.posum.scheduler.portfolio.PluginPolicy;
import org.apache.hadoop.tools.posum.simulation.core.appmaster.AMDaemon;
import org.apache.hadoop.tools.posum.simulation.core.appmaster.MRAMDaemon;
import org.apache.hadoop.tools.posum.simulation.core.daemon.DaemonPool;
import org.apache.hadoop.tools.posum.simulation.core.nodemanager.NMDaemon;
import org.apache.hadoop.tools.posum.simulation.core.nodemanager.SimulatedContainer;
import org.apache.hadoop.tools.posum.simulation.core.resourcemanager.SimulationResourceManager;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static org.apache.hadoop.mapreduce.MRJobConfig.COMPLETED_MAPS_FOR_REDUCE_SLOWSTART;
import static org.apache.hadoop.mapreduce.v2.api.records.TaskType.MAP;
import static org.apache.hadoop.mapreduce.v2.app.rm.RMContainerAllocator.DEFAULT_COMPLETED_MAPS_PERCENT_FOR_REDUCE_SLOWSTART;
import static org.apache.hadoop.tools.posum.common.util.conf.PosumConfiguration.AM_DAEMON_HEARTBEAT_INTERVAL_MS;
import static org.apache.hadoop.tools.posum.common.util.conf.PosumConfiguration.AM_DAEMON_HEARTBEAT_INTERVAL_MS_DEFAULT;
import static org.apache.hadoop.tools.posum.common.util.conf.PosumConfiguration.NM_DAEMON_HEARTBEAT_INTERVAL_MS;
import static org.apache.hadoop.tools.posum.common.util.conf.PosumConfiguration.NM_DAEMON_HEARTBEAT_INTERVAL_MS_DEFAULT;
import static org.apache.hadoop.tools.posum.common.util.conf.PosumConfiguration.NM_DAEMON_MEMORY_MB;
import static org.apache.hadoop.tools.posum.common.util.conf.PosumConfiguration.NM_DAEMON_MEMORY_MB_DEFAULT;
import static org.apache.hadoop.tools.posum.common.util.conf.PosumConfiguration.NM_DAEMON_VCORES;
import static org.apache.hadoop.tools.posum.common.util.conf.PosumConfiguration.NM_DAEMON_VCORES_DEFAULT;
import static org.apache.hadoop.tools.posum.common.util.conf.PosumConfiguration.SIMULATION_CONTAINER_MEMORY_MB;
import static org.apache.hadoop.tools.posum.common.util.conf.PosumConfiguration.SIMULATION_CONTAINER_MEMORY_MB_DEFAULT;
import static org.apache.hadoop.tools.posum.common.util.conf.PosumConfiguration.SIMULATION_CONTAINER_VCORES;
import static org.apache.hadoop.tools.posum.common.util.conf.PosumConfiguration.SIMULATION_CONTAINER_VCORES_DEFAULT;

public class SimulationRunner<T extends PluginPolicy> {
  private final static Logger LOG = Logger.getLogger(SimulationRunner.class);
  private final IdsByQueryCall GET_STARTED_JOBS = IdsByQueryCall.newInstance(DataEntityCollection.JOB, QueryUtils.isNot("startTime", null), "submitTime", false);
  private final IdsByQueryCall GET_NOTSTARTED_JOBS = IdsByQueryCall.newInstance(DataEntityCollection.JOB, QueryUtils.is("startTime", null));
  private final FindByIdCall GET_JOB = FindByIdCall.newInstance(DataEntityCollection.JOB, null);
  private final FindByIdCall GET_CONF = FindByIdCall.newInstance(DataEntityCollection.JOB_CONF, null);
  private final FindByQueryCall GET_TASKS = FindByQueryCall.newInstance(DataEntityCollection.TASK, null);

  private SimulationResourceManager rm;
  private DaemonPool daemonPool;
  private SimulationContext<T> context;
  private Configuration conf;
  private Resource containerResource;
  private Map<String, NMDaemon> nmMap;
  private Map<String, AMDaemon> amMap;

  public SimulationRunner(SimulationContext<T> context) throws IOException, ClassNotFoundException {
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
    prepareAMs();
    queueNMs();

    // blocked until all NMs are RUNNING
    waitForNodesRunning();

    queueAMs();
    // wait for apps to be running and assign containers that are already running
    addRunningContainers();

    daemonPool.start();
    context.getRemainingJobsCounter().await();
    daemonPool.shutDown();

    LOG.info("SimulationRunner finished for " + context.getSchedulerClass().getSimpleName());
  }

  private void prepareAMs() throws YarnException, IOException {
    int heartbeatInterval = conf.getInt(AM_DAEMON_HEARTBEAT_INTERVAL_MS, AM_DAEMON_HEARTBEAT_INTERVAL_MS_DEFAULT);

    Map<String, Integer> queueAppNumMap = new HashMap<>();

    Database sourceDb = context.getSourceDatabase();

    List<String> jobIds = new ArrayList<>(sourceDb.execute(GET_STARTED_JOBS).getEntries());
    jobIds.addAll(sourceDb.execute(GET_NOTSTARTED_JOBS).getEntries());
    amMap = new HashMap<>(jobIds.size());

    LOG.trace(MessageFormat.format("Sim={0}: Adding AMs for jobs {1}", context.getSchedulerClass().getSimpleName(), jobIds));
    for (String jobId : jobIds) {
      GET_JOB.setId(jobId);
      JobProfile job = sourceDb.execute(GET_JOB).getEntity();
      long jobSubmitTime = job.getSubmitTime();
      if (context.getClusterTimeAtStart() == 0) {
        context.setClusterTimeAtStart(jobSubmitTime);
        context.setCurrentTime(context.isOnlineSimulation() ? context.getCurrentTime() - jobSubmitTime : 0);
      }
      jobSubmitTime -= context.getClusterTimeAtStart();
      if (jobSubmitTime < 0) {
        LOG.warn(MessageFormat.format("Sim={0}: Warning: reset job {1} start time to 0", job.getId()));
        jobSubmitTime = 0;
      }

      String user = job.getUser();
      String queue = job.getQueue();
      String oldAppId = job.getAppId();
      int queueSize = queueAppNumMap.containsKey(queue) ? queueAppNumMap.get(queue) : 0;
      queueSize++;
      queueAppNumMap.put(queue, queueSize);

      GET_CONF.setId(job.getId());
      JobConfProxy jobConf = sourceDb.execute(GET_CONF).getEntity();
      float slowStartRatio = jobConf.getConf().getFloat(COMPLETED_MAPS_FOR_REDUCE_SLOWSTART,
        DEFAULT_COMPLETED_MAPS_PERCENT_FOR_REDUCE_SLOWSTART);

      MRAMDaemon amSim = new MRAMDaemon(context);
      amSim.init(heartbeatInterval, createContainers(jobId), rm, jobSubmitTime, user, queue, oldAppId, job.getHostName(), slowStartRatio);
      amMap.put(oldAppId, amSim);
    }

    LOG.trace(MessageFormat.format("Sim={0}: Cluster time at start is {1}", context.getSchedulerClass().getSimpleName(), context.getClusterTimeAtStart()));
    LOG.trace(MessageFormat.format("Sim={0}: Simulation start time is {1}", context.getSchedulerClass().getSimpleName(), context.getCurrentTime()));
    context.setRemainingJobsCounter(new CountDownLatch(amMap.size()));
  }

  private List<SimulatedContainer> createContainers(String jobId) {
    List<SimulatedContainer> ret = new ArrayList<>();

    GET_TASKS.setQuery(QueryUtils.is("jobId", jobId));
    List<TaskProfile> jobTasks = context.getSourceDatabase().execute(GET_TASKS).getEntities();

    for (TaskProfile task : jobTasks) {
      if (!context.isOnlineSimulation() || task.getFinishTime() == null) { // only schedule unfinished tasks if simulating online
        Long taskStart = task.getStartTime();
        Long taskFinish = task.getFinishTime();
        Long lifeTime = taskStart != null && taskFinish != null ? taskFinish - taskStart : null;
        int priority = 0;
        String type = task.getType() == MAP ? "map" : "reduce";
        Long originalStartTime = task.getStartTime() == null || !context.isOnlineSimulation() ? null :
          task.getStartTime() - context.getClusterTimeAtStart();
        ret.add(new SimulatedContainer(context, containerResource, lifeTime, priority, type,
          task.getId(), task.getSplitLocations(), originalStartTime, task.getHostName()));
      }
    }
    // sort so that started containers go first for allocation
    Collections.sort(ret, new Comparator<SimulatedContainer>() {
      @Override
      public int compare(SimulatedContainer o1, SimulatedContainer o2) {
        return o1.getOriginalStartTime() != null ? -1 : 1;
      }
    });
    return ret;
  }

  private void startRM() throws IOException, ClassNotFoundException {
    rm = new SimulationResourceManager<>(context);
    Configuration conf = PosumConfiguration.newInstance(new YarnConfiguration());
    conf.setBoolean(YarnConfiguration.RM_SCHEDULER_INCLUDE_PORT_IN_NODE_NAME, false);
    rm.init(conf);
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
    Random random = new Random();
    for (String hostName : activeNodes) {
      // randomize the start time from -heartbeatInterval to zero, in order to start NMs before AMs
      NMDaemon nm = new NMDaemon(context);
      nm.init(hostName,
        nmMemoryMB,
        nmVCores,
        -random.nextInt(heartbeatInterval),
        heartbeatInterval, rm
      );
      nmMap.put(hostName, nm);
      daemonPool.schedule(nm);
    }
  }

  private void queueAMs() {
    for (AMDaemon amDaemon : amMap.values()) {
      daemonPool.schedule(amDaemon);
    }
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

  private void addRunningContainers() throws Exception {
    for (AMDaemon am : amMap.values()) {
      if (context.isOnlineSimulation() && am.getHostName() != null) { // AM container should be running
        while (!am.isRegistered())
          Thread.sleep(100);
        ApplicationId appId = am.getAppId();
        (rm.getPluginPolicy()).forceContainerAssignment(appId, am.getHostName());
        am.doStep();
        for (SimulatedContainer container : am.getContainers()) {
          if (container.getOriginalStartTime() != null && container.getHostName() != null)
            (rm.getPluginPolicy()).forceContainerAssignment(appId, container.getHostName());
        }
      }
    }
  }
}
