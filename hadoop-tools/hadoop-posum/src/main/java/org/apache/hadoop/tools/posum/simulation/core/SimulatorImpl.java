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
import org.apache.hadoop.tools.posum.common.util.conf.PolicyPortfolio;
import org.apache.hadoop.tools.posum.scheduler.portfolio.PluginPolicy;
import org.apache.hadoop.tools.posum.simulation.master.SimulationMasterContext;
import org.apache.hadoop.tools.posum.simulation.predictor.JobBehaviorPredictor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.hadoop.tools.posum.common.util.Utils.copyRunningAppInfo;
import static org.apache.hadoop.util.ShutdownThreadsHelper.shutdownExecutorService;

public class SimulatorImpl extends CompositeService implements Simulator {

  private static Log logger = LogFactory.getLog(SimulatorImpl.class);

  private SimulationMasterContext context;
  private PolicyPortfolio policies;
  private JobBehaviorPredictor predictor;
  private ExecutorService executor;
  private ResultAggregator resultAggregator;
  private volatile boolean simulationOngoing = false;

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
    simulationOngoing = true;
    DataStore dataStore = context.getDataBroker();
    if (getRunningJobCount() < 1) {
      logger.debug("Queue is empty. No simulations will start");
      simulationsDone(Collections.<SimulationResultPayload>emptyList());
      return;
    }
    copyRunningAppInfo(dataStore, DatabaseReference.getMain(), DatabaseReference.getSimulation());
    predictor.train(Database.from(dataStore, DatabaseReference.getMain()));
    predictor.switchDatabase(Database.from(dataStore, DatabaseReference.getSimulation()));

    List<PendingResult> simulations = new ArrayList<>(policies.size());
    for (Map.Entry<String, Class<? extends PluginPolicy>> policy : policies.entrySet()) {
      logger.debug("Starting simulation for " + policy.getKey());
      Class<? extends PluginPolicy> policyClass = policy.getValue();
      // TODO add topology
      SimulationManager simulation = new SimulationManager(predictor, policy.getKey(), policyClass, dataStore, null, true);
      simulations.add(new PendingResult(simulation, executor.submit(simulation)));
    }
    resultAggregator = new ResultAggregator(simulations, this);
    executor.execute(resultAggregator);
  }

  @Override
  public synchronized void reset() {
    try {
      logger.debug("Simulator resetting...");
      if (simulationOngoing) {
        logger.debug("Shutting down simulations and executor...");

        resultAggregator.stop();
        try {
          shutdownExecutorService(executor);
        } catch (InterruptedException e) {
          logger.error("Simulator pool did not shut down correctly", e);
        }
        simulationsDone(Collections.<SimulationResultPayload>emptyList());
        executor = Executors.newFixedThreadPool(policies.size());
      }
      predictor.clearHistory();
      logger.debug("Reset successful");
    } catch (Exception e) {
      logger.error("An error occurred while resetting simulator", e);
    }
  }

  private int getRunningJobCount() {
    IdsByQueryCall allJobs = IdsByQueryCall.newInstance(DataEntityCollection.JOB, null);
    return context.getDataBroker().execute(allJobs, DatabaseReference.getMain()).getEntries().size();
  }

  void simulationsDone(List<SimulationResultPayload> results) {
    HandleSimResultRequest resultRequest = HandleSimResultRequest.newInstance();
    resultRequest.setResults(results);
    logger.trace("Sending simulation result request");
    String message = results.size() > 0 ? "Simulation results: " + results : "Simulation was not performed";
    context.getDataBroker().execute(StoreLogCall.newInstance(message), null);
    context.getCommService().getOrchestratorMaster().handleSimulationResult(resultRequest);
    simulationOngoing = false;
  }

  @Override
  protected void serviceStop() throws Exception {
    shutdownExecutorService(executor);
    super.serviceStop();
  }
}
