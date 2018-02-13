package org.apache.hadoop.tools.posum.simulation.predictor.simple;

import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.simulation.predictor.PredictionStatEntry;
import org.apache.hadoop.tools.posum.simulation.predictor.PredictionStats;

import java.util.HashMap;
import java.util.Map;

public abstract class SimpleMRPredictionModel<
  M extends PredictionStats<E>,
  R extends PredictionStats<E>,
  E extends PredictionStatEntry<E>,
  P extends SimpleMRPredictionProfile<M, R, E>> extends SimplePredictionModel<P> {

  private Map<String, M> mapStatsDictionary = new HashMap<>();
  private Map<String, R> reduceStatsDictionary = new HashMap<>();

  public SimpleMRPredictionModel(int historyBuffer) {
    super(historyBuffer);
  }

  @Override
  public void updateModel(P predictionProfile) {
    super.updateModel(predictionProfile);
    JobProfile job = predictionProfile.getJob();
    updateStatsByType(predictionProfile, job.getMapperClass(), job.getReducerClass(), 1);
    updateStatsByType(predictionProfile, job.getUser(), job.getUser(), 2);
  }

  private void updateStatsByType(P predictionProfile,
                                 String mapKey,
                                 String reduceKey,
                                 int relevance) {
    M mapStats = mapStatsDictionary.get(mapKey);
    if (mapStats == null) {
      mapStats = newMapStats(relevance);
      mapStatsDictionary.put(mapKey, mapStats);
    }
    mapStats.merge(predictionProfile.getMapStats());
    if (predictionProfile.getJob().getTotalReduceTasks() > 0) {
      R reduceStats = reduceStatsDictionary.get(reduceKey);
      if (reduceStats == null) {
        reduceStats = newReduceStats(relevance);
        reduceStatsDictionary.put(reduceKey, reduceStats);
      }
      reduceStats.merge(predictionProfile.getReduceStats());
    }
  }

  protected abstract M newMapStats(int relevance);

  protected abstract R newReduceStats(int relevance);

  public M getRelevantMapStats(JobProfile job) {
    return getRelevantMapStats(job.getMapperClass(), job.getUser());
  }

  private M getRelevantMapStats(String mapClass, String user) {
    M classStats = mapStatsDictionary.get(mapClass);
    if (classStats != null)
      return classStats;
    return mapStatsDictionary.get(user);
  }

  public R getRelevantReduceStats(JobProfile job) {
    return getRelevantReduceStats(job.getReducerClass(), job.getUser());
  }

  private R getRelevantReduceStats(String reduceClass, String user) {
    R classStats = reduceStatsDictionary.get(reduceClass);
    if (classStats != null)
      return classStats;
    return reduceStatsDictionary.get(user);
  }
}
