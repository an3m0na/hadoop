package org.apache.hadoop.tools.posum.simulation.predictor.basic;

import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.simulation.predictor.simple.SimplePredictionModel;

import java.util.HashMap;
import java.util.Map;

class BasicPredictionModel extends SimplePredictionModel {

  private Map<String, BasicPredictionStats> statsByUser = new HashMap<>();

  public BasicPredictionModel(int historyBuffer) {
    super(historyBuffer);
  }

  @Override
  public void updateModel(JobProfile job) {
    updateStats(job);
    sourceJobs.add(job.getId());
  }

  private void updateStats(JobProfile job) {
    BasicPredictionStats stats = statsByUser.get(job.getUser());
    if (stats == null) {
      stats = new BasicPredictionStats(historyBuffer, 1);
      statsByUser.put(job.getUser(), stats);
    }
    stats.addSource(job);
  }

  public BasicPredictionStats getRelevantStats(JobProfile job) {
    return statsByUser.get(job.getUser());
  }
}
