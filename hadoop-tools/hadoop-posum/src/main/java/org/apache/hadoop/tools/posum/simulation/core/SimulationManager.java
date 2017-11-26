package org.apache.hadoop.tools.posum.simulation.core;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.tools.posum.client.data.DataStore;
import org.apache.hadoop.tools.posum.client.data.Database;
import org.apache.hadoop.tools.posum.client.data.DatabaseUtils;
import org.apache.hadoop.tools.posum.common.records.call.FindByQueryCall;
import org.apache.hadoop.tools.posum.common.records.dataentity.DatabaseReference;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.payload.SimulationResultPayload;
import org.apache.hadoop.tools.posum.common.util.cluster.PerformanceEvaluator;
import org.apache.hadoop.tools.posum.common.util.cluster.TopologyProvider;
import org.apache.hadoop.tools.posum.scheduler.portfolio.PluginPolicy;
import org.apache.hadoop.tools.posum.simulation.core.dispatcher.ApplicationEventType;
import org.apache.hadoop.tools.posum.simulation.core.dispatcher.ApplicationMonitor;
import org.apache.hadoop.tools.posum.simulation.core.dispatcher.ContainerEventType;
import org.apache.hadoop.tools.posum.simulation.core.dispatcher.ContainerMonitor;
import org.apache.hadoop.tools.posum.simulation.predictor.JobBehaviorPredictor;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.JOB;
import static org.apache.hadoop.tools.posum.common.util.GeneralUtils.orZero;


class SimulationManager<T extends PluginPolicy> implements Callable<SimulationResultPayload> {
  private static final Log logger = LogFactory.getLog(SimulationManager.class);

  private volatile boolean exit = false;
  private String policyName;
  private JobBehaviorPredictor predictor;
  private DataStore dataStore;
  private Database db;
  private Database sourceDb;
  private SimulationStatistics stats;
  private static final FindByQueryCall GET_LATEST =
    FindByQueryCall.newInstance(JOB, null, "lastUpdated", true, 0, 1);
  private SimulationContext<T> simulationContext;
  private PerformanceEvaluator performanceEvaluator;

  SimulationManager(JobBehaviorPredictor predictor,
                    String policyName,
                    Class<T> policyClass,
                    DataStore dataStore,
                    Map<String, String> topology,
                    boolean onlineSimulation) {
    this.predictor = predictor;
    this.policyName = policyName;
    this.dataStore = dataStore;
    stats = new SimulationStatistics();
    simulationContext = new SimulationContext<>(policyClass);
    performanceEvaluator = new PerformanceEvaluator(simulationContext);
    TopologyProvider topologyProvider = topology == null ? new TopologyProvider(simulationContext.getConf(), dataStore) :
      new TopologyProvider(topology);
    simulationContext.setTopologyProvider(topologyProvider);
    simulationContext.setOnlineSimulation(onlineSimulation);
  }

  public String getPolicyName() {
    return policyName;
  }

  private void setUp() {
    sourceDb = Database.from(dataStore, DatabaseReference.getSimulation());
    simulationContext.setSourceDatabase(sourceDb);

    DatabaseReference dbRef = DatabaseReference.get(DatabaseReference.Type.SIMULATION, policyName);
    dataStore.clearDatabase(dbRef);
    db = Database.from(dataStore, dbRef);
    simulationContext.setDatabase(db);

    simulationContext.setPredictor(predictor);
    stats.setStartTimeCluster(getLastUpdated());
    simulationContext.setCurrentTime(stats.getStartTimeCluster());
    stats.setStartTimePhysical(System.currentTimeMillis());

    simulationContext.getDispatcher().register(ContainerEventType.class, new ContainerMonitor(simulationContext, db));
    simulationContext.getDispatcher().register(ApplicationEventType.class, new ApplicationMonitor(simulationContext, db, sourceDb));
  }

  private void tearDown() {
    stats.setEndTimeCluster(stats.getStartTimeCluster() + simulationContext.getCurrentTime());
    stats.setEndTimePhysical(System.currentTimeMillis());
    //TODO log stats
  }

  private long getLastUpdated() {
    List<JobProfile> latest = sourceDb.execute(GET_LATEST).getEntities();
    if (latest != null && latest.size() > 0)
      return orZero(latest.get(0).getLastUpdated());
    return 0;
  }

  public void stop() {
    exit = true;
  }

  @Override
  public SimulationResultPayload call() throws Exception {
    setUp();
    try {
      DatabaseUtils.storeLogEntry("Starting simulation for " + policyName, dataStore);
      new SimulationRunner<>(simulationContext).run();
      SimulationResultPayload result = SimulationResultPayload.newInstance(policyName, performanceEvaluator.evaluate());
      DatabaseUtils.storeLogEntry("Score for simulation of " + policyName + ": " + result, dataStore);
      return result;
    } catch (InterruptedException e) {
      if (!exit)
        // exiting was not intentional
        logger.error("Simulation for " + policyName + "was interrupted unexpectedly", e);
      return SimulationResultPayload.newInstance(policyName, null);
    } catch (Exception e) {
      logger.error("Error during simulation for " + policyName + ". Shutting down simulation...", e);
      return SimulationResultPayload.newInstance(policyName, null);
    } finally {
      tearDown();
    }
  }

}
