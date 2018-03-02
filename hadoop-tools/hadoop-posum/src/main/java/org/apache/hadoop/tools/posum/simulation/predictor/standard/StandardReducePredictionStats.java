package org.apache.hadoop.tools.posum.simulation.predictor.standard;

import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.simulation.predictor.stats.AveragingStatEntry;
import org.apache.hadoop.tools.posum.simulation.predictor.stats.PredictionStats;

import static org.apache.hadoop.tools.posum.simulation.predictor.simple.SimpleStatKeys.REDUCE_DURATION;
import static org.apache.hadoop.tools.posum.simulation.predictor.standard.StandardStatKeys.REDUCE_RATE;

class StandardReducePredictionStats extends PredictionStats<AveragingStatEntry> {

  StandardReducePredictionStats(int relevance) {
    super(relevance, REDUCE_DURATION, REDUCE_RATE);
  }

  public void addSamples(JobProfile job) {
    int sampleNo = job.getCompletedReduces();
    Long avgDuration = job.getAvgReduceDuration();

    if (sampleNo > 0 && avgDuration != null) {
      addEntry(REDUCE_DURATION, new AveragingStatEntry(avgDuration, sampleNo));
      if (job.getReduceInputBytes() != null) {
        Double avgInputSize = Math.max(1.0 * job.getReduceInputBytes() / sampleNo, 1.0);
        addEntry(REDUCE_RATE, new AveragingStatEntry(avgInputSize / avgDuration, sampleNo));
      }
    }
  }

  @Override
  protected AveragingStatEntry emptyEntry() {
    return new AveragingStatEntry();
  }
}
