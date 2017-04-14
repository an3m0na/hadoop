package org.apache.hadoop.tools.posum.simulation.predictor.standard;

import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.simulation.predictor.PredictionStats;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

import static org.apache.hadoop.tools.posum.simulation.predictor.standard.FlexKeys.REDUCE_RATE;

class StandardReducePredictionStats extends PredictionStats {
  private Double avgReduceDuration;
  private Queue<Double> reduceDurations;
  private Double avgReduceRate;
  private Queue<Double> reduceRates;

  public StandardReducePredictionStats(int maxHistory, int relevance) {
    super(maxHistory, relevance);
    this.reduceDurations = new ArrayBlockingQueue<>(maxHistory);
    this.reduceRates = new ArrayBlockingQueue<>(maxHistory);
  }

  public Double getAvgReduceRate() {
    return avgReduceRate;
  }

  public void setAvgReduceRate(Double avgReduceRates) {
    this.avgReduceRate = avgReduceRates;
  }

  public Queue<Double> getReduceRates() {
    return reduceRates;
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

  public void updateStatsFromFlexFields(Map<String, String> flexFields) {
    avgReduceRate = addValue(
      flexFields.get(REDUCE_RATE.getKey()),
      avgReduceRate,
      reduceRates
    );
  }

  public void addSource(JobProfile job) {
    updateStatsFromFlexFields(job.getFlexFields());
    avgReduceDuration = addValue(job.getAvgReduceDuration().doubleValue(), avgReduceDuration, reduceDurations);
    incrementSampleSize();
  }

}
