package org.apache.hadoop.tools.posum.simulation.core;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.tools.posum.client.data.DataStore;
import org.apache.hadoop.tools.posum.client.data.Database;
import org.apache.hadoop.tools.posum.common.records.call.FindByIdCall;
import org.apache.hadoop.tools.posum.common.records.call.FindByQueryCall;
import org.apache.hadoop.tools.posum.common.records.call.IdsByQueryCall;
import org.apache.hadoop.tools.posum.common.records.call.query.QueryUtils;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.DatabaseReference;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.TaskProfile;
import org.apache.hadoop.tools.posum.common.records.payload.CompoundScorePayload;
import org.apache.hadoop.tools.posum.common.records.payload.SimulationResultPayload;
import org.apache.hadoop.tools.posum.simulation.master.SimulationMaster;
import org.apache.hadoop.tools.posum.simulation.predictor.JobBehaviorPredictor;
import org.apache.hadoop.tools.posum.simulation.predictor.TaskPredictionInput;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.hadoop.tools.posum.common.util.Utils.orZero;
import static org.apache.hadoop.tools.posum.simulation.core.SimulationEvent.Type.TASK_FINISHED;


class Simulation implements Callable<SimulationResultPayload> {
  private static final Log logger = LogFactory.getLog(SimulationMaster.class);

  private volatile boolean exit = false;
  private String policy;
  private JobBehaviorPredictor predictor;
  private DataStore dataStore;
  private DatabaseReference dbReference;
  private Database db;
  private SimulationStatistics stats;
  private static final FindByQueryCall GET_LATEST =
    FindByQueryCall.newInstance(DataEntityCollection.JOB, null, "lastUpdated", true, 0, 1);
  private Queue<SimulationEvent> eventQueue;
  private Long clusterTime = 0L;
  private Integer pendingJobs = 0;
  private Double runtime = 0.0;
  private Double penalty = 0.0;
  private Double cost = 0.0;


  Simulation(JobBehaviorPredictor predictor, String policy, DataStore dataStore) {
    this.predictor = predictor;
    this.policy = policy;
    this.dataStore = dataStore;
    this.stats = new SimulationStatistics();
    this.eventQueue = new LinkedBlockingQueue<>();
  }

  private void setUp() {
    dbReference = DatabaseReference.get(DatabaseReference.Type.SIMULATION, policy);
    dataStore.clearDatabase(dbReference);
    dataStore.copyDatabase(DatabaseReference.getSimulation(), dbReference);
    db = Database.from(dataStore, dbReference);
    predictor.initialize(db);
    stats.setStartTimeCluster(getLastUpdated());
    stats.setStartTimePhysical(System.currentTimeMillis());
    IdsByQueryCall getPendingJobs =
      IdsByQueryCall.newInstance(DataEntityCollection.JOB, QueryUtils.is("finishTime", 0L));
    pendingJobs = db.execute(getPendingJobs).getEntries().size();
  }

  private void tearDown() {
    stats.setEndTimeCluster(getLastUpdated());
    stats.setEndTimePhysical(System.currentTimeMillis());
    dataStore.clearDatabase(dbReference);
    //TODO log stats
  }

  private Long getLastUpdated() {
    List<JobProfile> latest = dataStore.execute(GET_LATEST, dbReference).getEntities();
    if (latest != null && latest.size() > 0)
      return orZero(latest.get(0).getLastUpdated());
    return null;
  }

  @Override
  public SimulationResultPayload call() throws Exception {
    setUp();
    try {
      initializeDaemonSimulators();
      loadInitialEvents();
      processQueue();
      return SimulationResultPayload.newInstance(policy, CompoundScorePayload.newInstance(runtime, penalty, cost));
    } catch (Exception e) {
      logger.error("Error during simulation. Shutting down simulation...", e);
      return SimulationResultPayload.newInstance(policy, CompoundScorePayload.newInstance(0.0, 0.0, 0.0));
    } finally {
      tearDown();
    }
  }

  private void initializeDaemonSimulators() {
    // TODO initialize the state of the hadoop daemon simulators (ResourceManager, ApplicationMasters and NodeManagers)
  }

  private void loadInitialEvents() {
    FindByQueryCall findAllocatedTasks =
      FindByQueryCall.newInstance(DataEntityCollection.TASK, QueryUtils.isNot("httpAddress", null));

    List<TaskProfile> allocatedTasks = db.execute(findAllocatedTasks).getEntities();
    for (TaskProfile allocatedTask : allocatedTasks) {
      Long duration = predictor.predictTaskDuration(new TaskPredictionInput(allocatedTask.getId())).getDuration();
      Float progress = allocatedTask.getReportedProgress();
      if (progress != null && progress > 0) {
        Float timeLeft = (1 - progress) * duration;
        duration = timeLeft.longValue();
      }
      eventQueue.add(new SimulationEvent<>(TASK_FINISHED, clusterTime + duration, new TaskFinishedDetails(allocatedTask.getId())));
    }
    // TODO for all other nodes that do not have a task running on them, send NODE_FREE events to the scheduler
  }

  private void processQueue() {
    FindByIdCall getTask = FindByIdCall.newInstance(DataEntityCollection.TASK, null);
    FindByIdCall getJob = FindByIdCall.newInstance(DataEntityCollection.JOB, null);
    while (pendingJobs > 0 && !exit) {
      SimulationEvent event = eventQueue.poll();
      clusterTime = event.getTimestamp();
      switch (event.getType()) {
        case TASK_FINISHED:
          getTask.setId(((TaskFinishedDetails) event.getDetails()).getTaskId());
          TaskProfile task = db.execute(getTask).getEntity();
          task.setFinishTime(clusterTime);
          // update other task details

          JobProfile job = db.execute(getJob).getEntity();
          if (checkLastTask(task, job)) {
            pendingJobs--;
            job.setFinishTime(clusterTime);
            // update other job details
            //TODO correctly update metrics
            runtime = Math.random() * 10;
            penalty++;
            cost++;
          }
          // TODO update daemon simulators about task + job (job event is necessary?)
          break;
      }
    }
  }

  private boolean checkLastTask(TaskProfile task, JobProfile job) {
    if (task.getType() == TaskType.REDUCE)
      return orZero(job.getTotalReduceTasks()) - orZero(job.getCompletedReduces()) == 1;
    return orZero(job.getTotalMapTasks()) - orZero(job.getCompletedMaps()) == 1;
  }
}
