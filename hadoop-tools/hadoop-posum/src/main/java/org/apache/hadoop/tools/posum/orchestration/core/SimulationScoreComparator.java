package org.apache.hadoop.tools.posum.orchestration.core;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.tools.posum.common.records.payload.CompoundScorePayload;
import org.apache.hadoop.tools.posum.common.records.payload.SimulationResultPayload;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class SimulationScoreComparator implements Comparator<SimulationResultPayload> {
  private static Log logger = LogFactory.getLog(SimulationScoreComparator.class);

  private Double slowdownScaleFactor, penaltyScaleFactor, costScaleFactor;

  public Double getSlowdownScaleFactor() {
    return slowdownScaleFactor;
  }

  public Double getPenaltyScaleFactor() {
    return penaltyScaleFactor;
  }

  public Double getCostScaleFactor() {
    return costScaleFactor;
  }

  public synchronized void updateScaleFactors(Double slowdownScaleFactor, Double penaltyScaleFactor, Double costScaleFactor) {
    this.slowdownScaleFactor = slowdownScaleFactor;
    this.penaltyScaleFactor = penaltyScaleFactor;
    this.costScaleFactor = costScaleFactor;
    logger.info("Scale factors updated to: " + slowdownScaleFactor + " " + penaltyScaleFactor + " " + costScaleFactor);
  }

  @Override
  public int compare(SimulationResultPayload r1, SimulationResultPayload r2) {
    // if they refer to the same policy, they are considered equal
    if (r1.getPolicyName().equals(r2.getPolicyName()))
      return 0;
    // if there is no info on one of them, the other is first
    if (r1.getScore() == null)
      return 1;
    if (r2.getScore() == null)
      return -1;
    CompoundScorePayload difference = r1.getScore().subtract(r2.getScore());
    // if the proportional difference is positive, the second is "smaller", so it goes first
    return getSlowdownScaleFactor() * difference.getSlowdown() +
      getPenaltyScaleFactor() * difference.getPenalty() +
      getCostScaleFactor() * difference.getCost() > 0 ? 1 : -1;
  }

  public synchronized void sort(List<SimulationResultPayload> results) {
    Collections.sort(results, this);
  }
}
