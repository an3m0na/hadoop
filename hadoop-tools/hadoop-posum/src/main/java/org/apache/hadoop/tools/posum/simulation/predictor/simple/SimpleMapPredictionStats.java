package org.apache.hadoop.tools.posum.simulation.predictor.simple;

import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.simulation.predictor.PredictionStats;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

public class SimpleMapPredictionStats extends PredictionStats {
  private Double avgRate;
  private Queue<Double> rates;
  private Double avgSelectivity;
  private Queue<Double> selectivities;
  private final String rateKey;
  private final String selectivityKey;

  public SimpleMapPredictionStats(int maxHistory, int relevance, String rateKey, String selectivityKey) {
    super(maxHistory, relevance);
    this.rates = new ArrayBlockingQueue<>(maxHistory);
    this.selectivities = new ArrayBlockingQueue<>(maxHistory);
    this.rateKey = rateKey;
    this.selectivityKey = selectivityKey;
  }

  public Double getAvgRate() {
    return avgRate;
  }

  public void setAvgRate(Double avgRate) {
    this.avgRate = avgRate;
  }

  public Queue<Double> getRates() {
    return rates;
  }

  public Double getAvgSelectivity() {
    return avgSelectivity;
  }

  public void setAvgSelectivity(Double avgSelectivity) {
    this.avgSelectivity = avgSelectivity;
  }

  public Queue<Double> getSelectivities() {
    return selectivities;
  }

  public void updateStatsFromFlexFields(Map<String, String> flexFields) {
    avgRate = addValue(
      flexFields.get(rateKey),
      avgRate,
      rates
    );
    avgSelectivity = addValue(
      flexFields.get(selectivityKey),
      avgSelectivity,
      selectivities
    );
  }

  public void addSource(JobProfile job) {
    updateStatsFromFlexFields(job.getFlexFields());
    incrementSampleSize();
  }
}
