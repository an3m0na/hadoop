package org.apache.hadoop.tools.posum.simulation.predictor.simple;

import org.apache.hadoop.tools.posum.simulation.predictor.PredictionModel;
import org.apache.hadoop.tools.posum.simulation.predictor.PredictionProfile;

import java.util.HashSet;
import java.util.Set;

public abstract class SimplePredictionModel<P extends PredictionProfile> implements PredictionModel<P> {

  protected int historyBuffer;
  protected Set<String> sourceJobs = new HashSet<>();

  public SimplePredictionModel(int historyBuffer) {
    // TODO do something with the history buffer (keep a window of statistics)
    this.historyBuffer = historyBuffer;
  }

  public Set<String> getSourceJobs() {
    return sourceJobs;
  }

  public void updateModel(P predictionProfile) {
    sourceJobs.add(predictionProfile.getJob().getId());
  }
}
