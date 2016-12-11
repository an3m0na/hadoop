package org.apache.hadoop.tools.posum.simulation.core;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.tools.posum.common.records.payload.SimulationResultPayload;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class ResultAggregator implements Runnable {
    private static Log logger = LogFactory.getLog(ResultAggregator.class);


    private volatile boolean exit = false;
    private Collection<PendingResult> pendingResults;
    private SimulatorImpl simulator;
    private List<SimulationResultPayload> finalResults;

    ResultAggregator(Collection<PendingResult> pendingResults,
                     SimulatorImpl simulator) {
        this.pendingResults = pendingResults;
        this.simulator = simulator;
        this.finalResults = new ArrayList<>(pendingResults.size());
    }

    @Override
    public void run() {
        Iterator<PendingResult> pendingIterator = pendingResults.iterator();
        while (!exit && pendingIterator.hasNext()) {
            awaitResult(pendingIterator.next());
        }
    }

    private void awaitResult(PendingResult result) {
        try {
            finalResults.add(result.getResult().get());
            if (finalResults.size() == pendingResults.size())
                simulator.simulationsDone(finalResults);
        } catch (Exception e) {
            logger.error("Error processing simulation", e);
        }
    }
}
