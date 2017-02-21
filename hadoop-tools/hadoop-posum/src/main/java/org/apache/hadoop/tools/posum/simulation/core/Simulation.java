package org.apache.hadoop.tools.posum.simulation.core;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.tools.posum.client.data.DataStore;
import org.apache.hadoop.tools.posum.client.data.Database;
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import static org.apache.hadoop.tools.posum.common.util.Utils.orZero;


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

    SimulationContext context = new SimulationContext();
    //TODO fill it up
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
      loadInitialEvents();
      return SimulationResultPayload.newInstance(policy, CompoundScorePayload.newInstance(runtime, penalty, cost));
    } catch (Exception e) {
      logger.error("Error during simulation. Shutting down simulation...", e);
      return SimulationResultPayload.newInstance(policy, CompoundScorePayload.newInstance(0.0, 0.0, 0.0));
    } finally {
      tearDown();
    }
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
    }
    // TODO for all other nodes that do not have a task running on them, send NODE_FREE events to the scheduler
  }
}
