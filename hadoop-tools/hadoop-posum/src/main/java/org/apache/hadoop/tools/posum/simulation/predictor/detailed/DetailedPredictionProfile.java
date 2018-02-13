package org.apache.hadoop.tools.posum.simulation.predictor.detailed;

import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.simulation.predictor.simple.AveragingStatEntry;
import org.apache.hadoop.tools.posum.simulation.predictor.simple.SimpleMRPredictionProfile;

class DetailedPredictionProfile extends SimpleMRPredictionProfile<
  DetailedMapPredictionStats,
  DetailedReducePredictionStats,
  AveragingStatEntry> {
  DetailedPredictionProfile(JobProfile job,
                            DetailedMapPredictionStats mapStats,
                            DetailedReducePredictionStats reduceStats) {
    super(job, mapStats, reduceStats);
  }
}
