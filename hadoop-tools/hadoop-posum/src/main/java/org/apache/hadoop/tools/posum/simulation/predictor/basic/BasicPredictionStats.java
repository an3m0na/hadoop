package org.apache.hadoop.tools.posum.simulation.predictor.basic;

import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.simulation.predictor.simple.SimpleStatKeys;
import org.apache.hadoop.tools.posum.simulation.predictor.stats.AveragingStatEntryImpl;
import org.apache.hadoop.tools.posum.simulation.predictor.stats.PredictionStats;

import static org.apache.hadoop.tools.posum.simulation.predictor.simple.SimpleStatKeys.MAP_DURATION;
import static org.apache.hadoop.tools.posum.simulation.predictor.simple.SimpleStatKeys.REDUCE_DURATION;

class BasicPredictionStats extends PredictionStats<AveragingStatEntryImpl> {
  BasicPredictionStats(int relevance) {
    super(relevance, SimpleStatKeys.MAP_DURATION, REDUCE_DURATION);
  }

  void addSamples(JobProfile job) {
    if (job.getAvgMapDuration() != null)
      addEntry(MAP_DURATION, new AveragingStatEntryImpl(job.getAvgMapDuration(), job.getCompletedMaps()));
    if (job.getAvgReduceDuration() != null)
      addEntry(REDUCE_DURATION, new AveragingStatEntryImpl(job.getAvgReduceDuration(), job.getCompletedReduces()));
  }

  public Double getAverage(Enum key) {
    AveragingStatEntryImpl entry = getEntry(key);
    return entry == null ? null : entry.getAverage();
  }

  @Override
  protected AveragingStatEntryImpl emptyEntry() {
    return new AveragingStatEntryImpl();
  }
}
