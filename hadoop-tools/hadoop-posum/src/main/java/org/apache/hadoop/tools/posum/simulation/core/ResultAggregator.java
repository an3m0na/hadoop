package org.apache.hadoop.tools.posum.simulation.core;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.tools.posum.common.records.payload.SimulationResultPayload;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

class ResultAggregator implements Runnable {
  private static Log logger = LogFactory.getLog(ResultAggregator.class);

  private volatile boolean exit = false;
  private Collection<PendingResult> pendingResults;
  private SimulationResultHandler resultHandler;
  private List<SimulationResultPayload> finalResults;

  ResultAggregator(Collection<PendingResult> pendingResults,
                   SimulationResultHandler resultHandler) {
    this.pendingResults = pendingResults;
    this.resultHandler = resultHandler;
    this.finalResults = new ArrayList<>(pendingResults.size());
  }

  @Override
  public void run() {
    Iterator<PendingResult> pendingIterator = pendingResults.iterator();
    while (!exit && pendingIterator.hasNext()) {
      awaitResult(pendingIterator.next());
    }
  }

  public void stop() {
    for (PendingResult pendingResult : pendingResults) {
      pendingResult.getSimulation().stop();
    }
    exit = true;
  }

  private void awaitResult(PendingResult result) {
    try {
      finalResults.add(result.getResult().get());
    } catch (InterruptedException e) {
      if (!exit)
        // exiting was not intentional
        logger.error("Result aggregator was interrupted unexpectedly", e);
      finalResults.add(SimulationResultPayload.newInstance(result.getSimulation().getPolicyName(), null));
    } catch (Exception e) {
      logger.error("Error aggregating simulation results", e);
      finalResults.add(SimulationResultPayload.newInstance(result.getSimulation().getPolicyName(), null));
    }
    if (finalResults.size() == pendingResults.size())
      resultHandler.simulationsDone(finalResults);
  }
}
