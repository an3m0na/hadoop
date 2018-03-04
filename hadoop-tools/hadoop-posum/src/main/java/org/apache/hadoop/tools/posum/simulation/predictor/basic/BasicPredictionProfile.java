package org.apache.hadoop.tools.posum.simulation.predictor.basic;

import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.simulation.predictor.simple.SimplePredictionProfile;
import org.apache.hadoop.tools.posum.simulation.predictor.stats.AveragingStatEntryImpl;

public class BasicPredictionProfile extends SimplePredictionProfile<BasicPredictionStats, AveragingStatEntryImpl> {
  BasicPredictionProfile(JobProfile job, BasicPredictionStats newStats) {
    super(job, newStats);
  }
}