package org.apache.hadoop.tools.posum.simulation.predictor.simple;

import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.simulation.predictor.PredictionStats;

import java.util.HashMap;
import java.util.Map;

public abstract class SimpleMRPredictionModel<M extends PredictionStats, R extends PredictionStats> extends SimplePredictionModel {

  protected Class<M> mapStatsClass;
  protected Class<R> reduceStatsClass;
  private Map<String, M> mapStatsByUser = new HashMap<>();
  private Map<String, R> reduceStatsByUser = new HashMap<>();
  private Map<String, M> mapStatsByClass = new HashMap<>();
  private Map<String, R> reduceStatsByClass = new HashMap<>();

  public SimpleMRPredictionModel(Class<M> mapStatsClass, Class<R> reduceStatsClass, int historyBuffer) {
    super(historyBuffer);
    this.mapStatsClass = mapStatsClass;
    this.reduceStatsClass = reduceStatsClass;
    this.historyBuffer = historyBuffer;
  }

  @Override
  public void updateModel(JobProfile job) {
    updateStatsByType(job, mapStatsByClass, reduceStatsByClass, job.getMapperClass(), job.getReducerClass(), 1);
    updateStatsByType(job, mapStatsByUser, reduceStatsByUser, job.getUser(), job.getUser(), 2);
    sourceJobs.add(job.getId());
  }

  private void updateStatsByType(JobProfile job,
                                 Map<String, M> mapStatsDictionary,
                                 Map<String, R> reduceStatsDictionary,
                                 String mapKey,
                                 String reduceKey,
                                 int relevance) {
    M mapStats = mapStatsDictionary.get(mapKey);
    if (mapStats == null) {
      mapStats = newMapStats(historyBuffer, relevance);
      mapStatsDictionary.put(mapKey, mapStats);
    }
    mapStats.addSource(job);
    if (job.getTotalReduceTasks() > 0) {
      R reduceStats = reduceStatsDictionary.get(reduceKey);
      if (reduceStats == null) {
        reduceStats = newReduceStats(historyBuffer, relevance);
        reduceStatsDictionary.put(reduceKey, reduceStats);
      }
      reduceStats.addSource(job);
    }
  }

  protected abstract M newMapStats(int historyBuffer, int relevance);

  protected abstract R newReduceStats(int historyBuffer, int relevance);

  public M getRelevantMapStats(JobProfile job) {
    return getRelevantMapStats(job.getMapperClass(), job.getUser());
  }

  public M getRelevantMapStats(String mapClass, String user) {
    M classStats = mapStatsByClass.get(mapClass);
    if (classStats != null && classStats.getSampleSize() > 1)
      return classStats;
    return mapStatsByUser.get(user);
  }

  public R getRelevantReduceStats(JobProfile job) {
    return getRelevantReduceStats(job.getReducerClass(), job.getUser());
  }

  public R getRelevantReduceStats(String reduceClass, String user) {
    R classStats = reduceStatsByClass.get(reduceClass);
    if (classStats != null && classStats.getSampleSize() > 1)
      return classStats;
    return reduceStatsByUser.get(user);
  }

}
