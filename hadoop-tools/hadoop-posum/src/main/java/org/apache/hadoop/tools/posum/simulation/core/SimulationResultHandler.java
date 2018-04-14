package org.apache.hadoop.tools.posum.simulation.core;

import org.apache.hadoop.tools.posum.common.records.payload.SimulationResultPayload;

import java.util.List;

interface SimulationResultHandler {
  void simulationsDone(List<SimulationResultPayload> finalResults);
}
