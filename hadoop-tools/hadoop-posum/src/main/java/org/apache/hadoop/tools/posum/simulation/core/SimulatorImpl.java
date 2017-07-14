package org.apache.hadoop.tools.posum.simulation.core;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.tools.posum.client.data.DataStore;
import org.apache.hadoop.tools.posum.client.data.Database;
import org.apache.hadoop.tools.posum.client.simulation.Simulator;
import org.apache.hadoop.tools.posum.common.records.call.IdsByQueryCall;
import org.apache.hadoop.tools.posum.common.records.call.StoreLogCall;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.DatabaseReference;
import org.apache.hadoop.tools.posum.common.records.payload.SimulationResultPayload;
import org.apache.hadoop.tools.posum.common.records.request.HandleSimResultRequest;
import org.apache.hadoop.tools.posum.common.util.PolicyPortfolio;
import org.apache.hadoop.tools.posum.scheduler.portfolio.PluginPolicy;
import org.apache.hadoop.tools.posum.simulation.master.SimulationMasterContext;
import org.apache.hadoop.tools.posum.simulation.predictor.JobBehaviorPredictor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.tools.posum.common.util.Utils.copyRunningAppInfo;

public class SimulatorImpl extends CompositeService implements Simulator {

  private static Log logger = LogFactory.getLog(SimulatorImpl.class);

  private SimulationMasterContext context;
  private PolicyPortfolio policies;
  private Map<String, PendingResult> simulationMap;
  private JobBehaviorPredictor predictor;
  private ExecutorService executor;
  private ResultAggregator resultAggregator;

  public SimulatorImpl(SimulationMasterContext context) {
    super(SimulatorImpl.class.getName());
    this.context = context;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    predictor = JobBehaviorPredictor.newInstance(context.getConf());
    policies = new PolicyPortfolio(conf);
    executor = Executors.newFixedThreadPool(policies.size());

    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();
  }


  @Override
  public synchronized void startSimulation() {
    DataStore dataStore = context.getDataBroker();
    copyRunningAppInfo(dataStore, DatabaseReference.getMain(), DatabaseReference.getSimulation());
    if (getRunningJobCount() < 1) {
      logger.debug("Queue is empty. No simulations will start");
      return;
    }
    predictor.train(Database.from(dataStore, DatabaseReference.getMain()));
    predictor.switchDatabase(Database.from(dataStore, DatabaseReference.getSimulation()));

    simulationMap = new HashMap<>(policies.size());
    for (Map.Entry<String, Class<? extends PluginPolicy>> policy : policies.entrySet()) {
      logger.debug("Starting simulation for " + policy.getKey());
      Class<? extends PluginPolicy> policyClass = policy.getValue();
      // TODO add topology
      SimulationManager simulation = new SimulationManager(predictor, policy.getKey(), policyClass, dataStore, null);
      simulationMap.put(policy.getKey(), new PendingResult(simulation, executor.submit(simulation)));
    }
    resultAggregator = new ResultAggregator(simulationMap.values(), this);
    executor.execute(resultAggregator);
  }

  private int getRunningJobCount() {
    IdsByQueryCall allJobs = IdsByQueryCall.newInstance(DataEntityCollection.JOB, null);
    return context.getDataBroker().execute(allJobs, DatabaseReference.getSimulation()).getEntries().size();
  }

  void simulationsDone(List<SimulationResultPayload> results) {
    HandleSimResultRequest resultRequest = HandleSimResultRequest.newInstance();
    resultRequest.setResults(results);
    logger.trace("Sending simulation result request");
    context.getDataBroker().execute(StoreLogCall.newInstance("Simulation results: " + results), null);
    context.getCommService().getOrchestratorMaster().handleSimulationResult(resultRequest);
  }

  @Override
  protected void serviceStop() throws Exception {
    shutdownExecutor();
    super.serviceStop();
  }

  private void shutdownExecutor() {
    executor.shutdown(); // Disable new tasks from being submitted
    try {
      // Wait a while for existing tasks to terminate
      if (!executor.awaitTermination(20, TimeUnit.SECONDS)) {
        executor.shutdownNow(); // Cancel currently executing tasks
        // Wait a while for tasks to respond to being cancelled
        if (!executor.awaitTermination(10, TimeUnit.SECONDS))
          System.err.println("Pool did not terminate");
      }
    } catch (InterruptedException ie) {
      // (Re-)Cancel if current thread also interrupted
      executor.shutdownNow();
      // Preserve interrupt status
      Thread.currentThread().interrupt();
    }
  }
}
