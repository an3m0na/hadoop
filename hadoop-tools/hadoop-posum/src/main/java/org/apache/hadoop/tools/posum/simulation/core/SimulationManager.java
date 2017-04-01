package org.apache.hadoop.tools.posum.simulation.core;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.tools.posum.client.data.DataStore;
import org.apache.hadoop.tools.posum.client.data.Database;
import org.apache.hadoop.tools.posum.common.records.call.FindByIdCall;
import org.apache.hadoop.tools.posum.common.records.call.FindByQueryCall;
import org.apache.hadoop.tools.posum.common.records.call.IdsByQueryCall;
import org.apache.hadoop.tools.posum.common.records.call.UpdateOrStoreCall;
import org.apache.hadoop.tools.posum.common.records.call.query.QueryUtils;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.DatabaseReference;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.TaskProfile;
import org.apache.hadoop.tools.posum.common.records.payload.CompoundScorePayload;
import org.apache.hadoop.tools.posum.common.records.payload.SimulationResultPayload;
import org.apache.hadoop.tools.posum.common.util.PosumConfiguration;
import org.apache.hadoop.tools.posum.common.util.TopologyProvider;
import org.apache.hadoop.tools.posum.simulation.predictor.JobBehaviorPredictor;
import org.apache.hadoop.tools.posum.simulation.predictor.TaskPredictionInput;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import static org.apache.hadoop.tools.posum.common.util.Utils.orZero;


class Simulation implements Callable<SimulationResultPayload> {
  private static final Log logger = LogFactory.getLog(Simulation.class);

  private volatile boolean exit = false;
  private String policyName;
  private Class<? extends ResourceScheduler> policyClass;
  private JobBehaviorPredictor predictor;
  private DataStore dataStore;
  private DatabaseReference dbReference;
  private Database db;
  private SimulationStatistics stats;
  private static final FindByQueryCall GET_LATEST =
    FindByQueryCall.newInstance(DataEntityCollection.JOB, null, "lastUpdated", true, 0, 1);
  private Double runtime = 0.0;
  private Double penalty = 0.0;
  private Double cost = 0.0;
  private SimulationContext simulationContext;


  Simulation(JobBehaviorPredictor predictor, String policyName, Class<? extends ResourceScheduler> policyClass, DataStore dataStore, Map<String, String> topology) {
    this.predictor = predictor;
    this.policyName = policyName;
    this.policyClass = policyClass;
    this.dataStore = dataStore;
    this.stats = new SimulationStatistics();
    this.simulationContext = new SimulationContext();
    this.simulationContext.setTopologyProvider(new TopologyProvider(Collections.singletonMap(0L, topology)));
  }

  private void setUp() {
    dbReference = DatabaseReference.get(DatabaseReference.Type.SIMULATION, policyName);
    dataStore.clearDatabase(dbReference);
    dataStore.copyDatabase(DatabaseReference.getSimulation(), dbReference);
    db = Database.from(dataStore, dbReference);
    predictor.initialize(db);
    stats.setStartTimeCluster(getLastUpdated());
    stats.setStartTimePhysical(System.currentTimeMillis());
    simulationContext.setSchedulerClass(policyClass);
    simulationContext.setDatabase(db);
  }

  private void tearDown() {
    stats.setEndTimeCluster(stats.getStartTimeCluster() + simulationContext.getEndTime());
    stats.setEndTimePhysical(System.currentTimeMillis());
    dataStore.clearDatabase(dbReference);
    //TODO log stats
  }

  private Long getLastUpdated() {
    List<JobProfile> latest = dataStore.execute(GET_LATEST, dbReference).getEntities();
    if (latest != null && latest.size() > 0)
      return orZero(latest.get(0).getLastUpdated());
    return 0L;
  }

  @Override
  public SimulationResultPayload call() throws Exception {
    setUp();
    try {
      new SimulationRunner(simulationContext).run();
      return SimulationResultPayload.newInstance(policyName, CompoundScorePayload.newInstance(runtime, penalty, cost));
    } catch (Exception e) {
      logger.error("Error during simulation. Shutting down simulation...", e);
      return SimulationResultPayload.newInstance(policyName, CompoundScorePayload.newInstance(0.0, 0.0, 0.0));
    } finally {
      tearDown();
    }
  }
}
