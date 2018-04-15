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
import org.apache.hadoop.tools.posum.common.util.PosumException;
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
import static org.apache.hadoop.mapreduce.v2.app.rm.RMContainerAllocator.DEFAULT_COMPLETED_MAPS_PERCENT_FOR_REDUCE_SLOWSTART;
import static org.apache.hadoop.tools.posum.common.util.GeneralUtils.orZero;
import static org.apache.hadoop.tools.posum.common.util.conf.PosumConfiguration.AM_DAEMON_HEARTBEAT_INTERVAL_MS;
import static org.apache.hadoop.tools.posum.common.util.conf.PosumConfiguration.AM_DAEMON_HEARTBEAT_INTERVAL_MS_DEFAULT;
import static org.apache.hadoop.tools.posum.common.util.conf.PosumConfiguration.NM_DAEMON_HEARTBEAT_INTERVAL_MS;
import static org.apache.hadoop.tools.posum.common.util.conf.PosumConfiguration.NM_DAEMON_HEARTBEAT_INTERVAL_MS_DEFAULT;

public class SimulationRunner<T extends PluginPolicy> {
  private final static Logger LOG = Logger.getLogger(SimulationRunner.class);
  private final IdsByQueryCall GET_STARTED_JOBS = IdsByQueryCall.newInstance(DataEntityCollection.JOB,
      QueryUtils.and(
          QueryUtils.isNot("startTime", null),
          QueryUtils.isNot("startTime", null)
      ), "submitTime", false);
  private final IdsByQueryCall GET_NOTSTARTED_JOBS = IdsByQueryCall.newInstance(DataEntityCollection.JOB,
      QueryUtils.or(
          QueryUtils.is("startTime", null),
          QueryUtils.is("startTime", 0L)
      ), "submitTime", false);
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
  private SimulationSanityChecker sanityChecker;


  public SimulationRunner(SimulationContext<T> context) throws IOException, ClassNotFoundException {
    this.context = context;
    conf = context.getConf();
    daemonPool = new DaemonPool(context);
    int containerMemoryMB = conf.getInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB);
    int containerVCores = conf.getInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES, YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES);
    containerResource = BuilderUtils.newResource(containerMemoryMB, containerVCores);
    sanityChecker = new SimulationSanityChecker(conf);
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
    rm.stop();

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
        LOG.warn(MessageFormat.format("Sim={0}: Warning: reset job {1} start time to 0", context.getSchedulerClass().getSimpleName(), job.getId()));
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
      sanityChecker.countContainers(amSim);
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
      Long taskStart = orZero(task.getStartTime());
      Long taskFinish = orZero(task.getFinishTime());
      if (!context.isOnlineSimulation() || orZero(task.getFinishTime()) == 0) { // only schedule finished tasks if not simulating online
        Long lifeTime = taskStart != 0 && taskFinish != 0 ? taskFinish - taskStart : null;
        Long originalStartTime = taskStart == 0 ? null : task.getStartTime() - context.getClusterTimeAtStart();
        ret.add(new SimulatedContainer(context, containerResource, lifeTime, task.getType().name(),
            task.getId(), task.getSplitLocations(), originalStartTime, task.getHostName()));
      }
    }
    // sort so that containers that should already be allocated go first
    Collections.sort(ret, new Comparator<SimulatedContainer>() {
      @Override
      public int compare(SimulatedContainer o1, SimulatedContainer o2) {
        if (o1.getHostName() != null)
          return o2.getHostName() == null ? -1 : o1.getTaskId().compareTo(o2.getTaskId());
        return o2.getHostName() != null ? 1 : o1.getTaskId().compareTo(o2.getTaskId());
      }
    });
    return ret;
  }

  private void startRM() throws IOException, ClassNotFoundException {
    rm = new SimulationResourceManager<>(context);
    Configuration conf = new Configuration(this.conf);
    conf.setBoolean(YarnConfiguration.RM_SCHEDULER_INCLUDE_PORT_IN_NODE_NAME, false);
    rm.init(conf);
    rm.start();
  }

  private void queueNMs() throws YarnException, IOException {
    // nm configuration
    int nmMemoryMB = conf.getInt(YarnConfiguration.NM_PMEM_MB, YarnConfiguration.DEFAULT_NM_PMEM_MB);
    int nmVCores = conf.getInt(YarnConfiguration.NM_VCORES, YarnConfiguration.DEFAULT_NM_VCORES);
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
    if (!context.isOnlineSimulation())
      return;
    for (AMDaemon am : amMap.values()) {
      if (am.getHostName() != null) { // AM container should be running
        while (!am.isRegistered())
          Thread.sleep(100);
        ApplicationId appId = am.getAppId();
        if (!(rm.getPluginPolicy()).forceContainerAssignment(appId, am.getHostName()))
          throw new PosumException(MessageFormat.format("Sim={0}: Could not pre-assign AM container for {1}", context.getSchedulerClass().getSimpleName(), appId));
        LOG.debug(MessageFormat.format("Sim={0}: Pre-assigned AM container for {1}", context.getSchedulerClass().getSimpleName(), appId));
        am.doStep(); // process the AM container
        am.doStep(); // ask for task containers

        int preAssignedContainers = 0;
        for (SimulatedContainer container : am.getContainers()) {
          if (context.isOnlineSimulation() && container.getHostName() != null) {
            LOG.trace(MessageFormat.format("Sim={0}: Pre-assigning container for {1} on {2}", context.getSchedulerClass().getSimpleName(), container.getTaskId(), container.getHostName()));
            if (!(rm.getPluginPolicy()).forceContainerAssignment(appId, container.getHostName(), container.getPriority()))
              throw new PosumException(MessageFormat.format("Sim={0}: Could not pre-assign container for {1}", context.getSchedulerClass().getSimpleName(), container.getTaskId()));
            preAssignedContainers++;
          }
        }
        if (preAssignedContainers > 0)
          am.doStep(); // process task containers
      }
    }
  }
}
