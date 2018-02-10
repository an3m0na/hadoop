package org.apache.hadoop.tools.posum.simulation.predictor.standard;

import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.simulation.predictor.simple.SimpleMRPredictionProfile;

class StandardPredictionProfile extends SimpleMRPredictionProfile<StandardMapPredictionStats, StandardReducePredictionStats> {
  StandardPredictionProfile(JobProfile job, StandardMapPredictionStats mapStats, StandardReducePredictionStats reduceStats) {
    super(job, mapStats, reduceStats);
  }
}
