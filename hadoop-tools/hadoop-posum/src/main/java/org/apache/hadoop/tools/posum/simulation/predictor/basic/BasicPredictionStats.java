package org.apache.hadoop.tools.posum.simulation.predictor.basic;

import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.simulation.predictor.PredictionStats;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

public class BasicPredictionStats extends PredictionStats {
  private Double avgMapDuration;
  private Queue<Double> mapDurations;
  private Double avgReduceDuration;
  private Queue<Double> reduceDurations;

  public BasicPredictionStats(int maxHistory, int relevance) {
    super(maxHistory, relevance);
    this.mapDurations = new ArrayBlockingQueue<>(maxHistory);
    this.reduceDurations = new ArrayBlockingQueue<>(maxHistory);
  }

  @Override
  public void updateStatsFromFlexFields(Map<String, String> flexFields) {
    // no stats are in flex fields
  }

  public Double getAvgMapDuration() {
    return avgMapDuration;
  }

  public void setAvgMapDuration(Double avgMapDuration) {
    this.avgMapDuration = avgMapDuration;
  }

  public Queue<Double> getMapDurations() {
    return mapDurations;
  }

  public void setMapDurations(Queue<Double> mapDurations) {
    this.mapDurations = mapDurations;
  }

  public Double getAvgReduceDuration() {
    return avgReduceDuration;
  }

  public void setAvgReduceDuration(Double avgReduceDuration) {
    this.avgReduceDuration = avgReduceDuration;
  }

  public Queue<Double> getReduceDurations() {
    return reduceDurations;
  }

  public void setReduceDurations(Queue<Double> reduceDurations) {
    this.reduceDurations = reduceDurations;
  }

  public void addSource(JobProfile job) {
    avgMapDuration = addValue(job.getAvgMapDuration(), avgMapDuration, mapDurations);
    avgReduceDuration = addValue(job.getAvgReduceDuration(), avgReduceDuration, reduceDurations);
    incrementSampleSize();
  }

  @Override
  public String toString() {
    return "BasicPredictionStats{" +
      "avgMapDuration=" + avgMapDuration +
      ", mapDurations=" + mapDurations +
      ", avgReduceDuration=" + avgReduceDuration +
      ", reduceDurations=" + reduceDurations +
      '}';
  }
}
