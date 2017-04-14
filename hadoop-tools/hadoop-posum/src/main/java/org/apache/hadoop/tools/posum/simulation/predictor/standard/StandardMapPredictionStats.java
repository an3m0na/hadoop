package org.apache.hadoop.tools.posum.simulation.predictor.standard;

import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.simulation.predictor.PredictionStats;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

import static org.apache.hadoop.tools.posum.simulation.predictor.standard.FlexKeys.MAP_RATE;
import static org.apache.hadoop.tools.posum.simulation.predictor.standard.FlexKeys.MAP_SELECTIVITY;

public class StandardMapPredictionStats extends PredictionStats {
  private Double avgRate;
  private Queue<Double> rates;
  private Double avgSelectivity;
  private Queue<Double> selectivities;

  public StandardMapPredictionStats(int maxHistory, int relevance) {
    super(maxHistory, relevance);
    this.rates = new ArrayBlockingQueue<>(maxHistory);
    this.selectivities = new ArrayBlockingQueue<>(maxHistory);
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
      flexFields.get(MAP_RATE.getKey()),
      avgRate,
      rates
    );
    avgSelectivity = addValue(
      flexFields.get(MAP_SELECTIVITY.getKey()),
      avgSelectivity,
      selectivities
    );
  }

  public void addSource(JobProfile job) {
    updateStatsFromFlexFields(job.getFlexFields());
    incrementSampleSize();
  }

}
