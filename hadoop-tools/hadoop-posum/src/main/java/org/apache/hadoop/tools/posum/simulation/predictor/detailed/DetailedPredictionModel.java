package org.apache.hadoop.tools.posum.simulation.predictor.detailed;

import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DetailedPredictionModel {

  private int historyBuffer;
  private Map<String, DetailedMapPredictionStats> mapStatsByUser = new HashMap<>();
  private Map<String, DetailedReducePredictionStats> reduceStatsByUser = new HashMap<>();
  private Map<String, DetailedMapPredictionStats> mapStatsByClass = new HashMap<>();
  private Map<String, DetailedReducePredictionStats> reduceStatsByClass = new HashMap<>();
  private Set<String> sourceJobs = new HashSet<>();

  public DetailedPredictionModel(int historyBuffer) {
    this.historyBuffer = historyBuffer;
  }

  public Set<String> getSourceJobs() {
    return sourceJobs;
  }

  public void updateModel(List<JobProfile> source) {
    if (source == null || source.size() < 1)
      return;
    for (JobProfile job : source) {
      updateModel(job);
    }
  }

  public void updateModel(JobProfile job) {
    updateStatsByType(job, mapStatsByClass, reduceStatsByClass, job.getMapperClass(), job.getReducerClass(), 1);
    updateStatsByType(job, mapStatsByUser, reduceStatsByUser, job.getUser(), job.getUser(), 2);
    sourceJobs.add(job.getId());
  }

  private void updateStatsByType(JobProfile job,
                                 Map<String, DetailedMapPredictionStats> mapStatsDictionary,
                                 Map<String, DetailedReducePredictionStats> reduceStatsDictionary,
                                 String mapKey,
                                 String reduceKey,
                                 int relevance) {
    DetailedMapPredictionStats mapStats = mapStatsDictionary.get(mapKey);
    if (mapStats == null) {
      mapStats = new DetailedMapPredictionStats(historyBuffer, relevance);
      mapStatsDictionary.put(mapKey, mapStats);
    }
   mapStats.addSource(job);
    if (job.getTotalReduceTasks() > 0) {
      DetailedReducePredictionStats reduceStats = reduceStatsDictionary.get(reduceKey);
      if (reduceStats == null) {
        reduceStats = new DetailedReducePredictionStats(historyBuffer, relevance);
        reduceStatsDictionary.put(reduceKey, reduceStats);
      }
      reduceStats.addSource(job);
    }
  }

  public DetailedMapPredictionStats getRelevantMapStats(String mapClass, String user) {
    DetailedMapPredictionStats classStats = mapStatsByClass.get(mapClass);
    if (classStats != null && classStats.getSampleSize() > 1)
      return classStats;
    return mapStatsByUser.get(user);
  }

  public DetailedReducePredictionStats getRelevantReduceStats(String reduceClass, String user) {
    DetailedReducePredictionStats classStats = reduceStatsByClass.get(reduceClass);
    if (classStats != null && classStats.getSampleSize() > 1)
      return classStats;
    return reduceStatsByUser.get(user);
  }
}
