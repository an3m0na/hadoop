package org.apache.hadoop.tools.posum.simulator.master;

import org.apache.hadoop.tools.posum.common.records.payload.SimulationResultPayload;

import java.util.concurrent.Future;

/**
 * Created by ane on 12/10/16.
 */
public class PendingResult {
    private Simulation simulation;
    private Future<SimulationResultPayload> result;

    public PendingResult(Simulation simulation, Future<SimulationResultPayload> result) {
        this.simulation = simulation;
        this.result = result;
    }

    public Simulation getSimulation() {
        return simulation;
    }

    public Future<SimulationResultPayload> getResult() {
        return result;
    }
}
