package org.apache.hadoop.tools.posum.simulation.core;

import org.apache.hadoop.tools.posum.common.records.payload.SimulationResultPayload;

import java.util.concurrent.Future;

class PendingResult {
    private Simulation simulation;
    private Future<SimulationResultPayload> result;

    PendingResult(Simulation simulation, Future<SimulationResultPayload> result) {
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
