package org.apache.hadoop.tools.posum.simulation.predictor.basic;

import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.simulation.predictor.simple.SimpleStatKeys;
import org.apache.hadoop.tools.posum.simulation.predictor.stats.AveragingStatEntry;
import org.apache.hadoop.tools.posum.simulation.predictor.stats.PredictionStats;

import static org.apache.hadoop.tools.posum.simulation.predictor.simple.SimpleStatKeys.MAP_DURATION;
import static org.apache.hadoop.tools.posum.simulation.predictor.simple.SimpleStatKeys.REDUCE_DURATION;

class BasicPredictionStats extends PredictionStats<AveragingStatEntry> {
  BasicPredictionStats(int relevance) {
    super(relevance, SimpleStatKeys.MAP_DURATION, REDUCE_DURATION);
  }

  void addSamples(JobProfile job) {
    if (job.getAvgMapDuration() != null)
      addEntry(MAP_DURATION, new AveragingStatEntry(job.getAvgMapDuration(), job.getCompletedMaps()));
    if (job.getAvgReduceDuration() != null)
      addEntry(REDUCE_DURATION, new AveragingStatEntry(job.getAvgReduceDuration(), job.getCompletedReduces()));
  }

  public Double getAverage(Enum key) {
    AveragingStatEntry entry = getEntry(key);
    return entry == null ? null : entry.getAverage();
  }

  @Override
  protected AveragingStatEntry emptyEntry() {
    return new AveragingStatEntry();
  }
}
