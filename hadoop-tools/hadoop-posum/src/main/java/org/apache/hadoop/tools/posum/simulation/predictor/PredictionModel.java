package org.apache.hadoop.tools.posum.simulation.predictor;

import java.util.Set;

public interface PredictionModel<P extends PredictionProfile> {

  Set<String> getSourceJobs();

  void updateModel(P predictionProfile);
}
