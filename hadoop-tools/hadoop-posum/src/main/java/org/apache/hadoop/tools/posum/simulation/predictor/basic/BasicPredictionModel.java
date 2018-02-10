package org.apache.hadoop.tools.posum.simulation.predictor.basic;

import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.simulation.predictor.simple.SimplePredictionModel;

import java.util.HashMap;
import java.util.Map;

class BasicPredictionModel extends SimplePredictionModel<BasicPredictionProfile> {

  private Map<String, BasicPredictionStats> statsByUser = new HashMap<>();

  BasicPredictionModel(int historyBuffer) {
    super(historyBuffer);
  }

  @Override
  public void updateModel(BasicPredictionProfile predictionProfile) {
    super.updateModel(predictionProfile);
    updateStats(predictionProfile);
  }

  private void updateStats(BasicPredictionProfile predictionProfile) {
    String user = predictionProfile.getJob().getUser();
    BasicPredictionStats stats = statsByUser.get(user);
    if (stats == null) {
      stats = new BasicPredictionStats(1);
      statsByUser.put(user, stats);
    }
    stats.merge(predictionProfile.getStats());
  }

  public BasicPredictionStats getRelevantStats(JobProfile job) {
    return statsByUser.get(job.getUser());
  }
}
