package org.apache.hadoop.tools.posum.simulation.predictor.detailed;

import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.Common.FLEX_KEY_PREFIX;
import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.Common.FlexKeys.MAP_GENERAL;
import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.Common.FlexKeys.MAP_LOCAL;
import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.Common.FlexKeys.MAP_REMOTE;
import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.Common.FlexKeys.MAP_SELECTIVITY;

class DetailedMapPredictionStats extends DetailedPredictionStats {
  private Double avgRate;
  private Queue<Double> rates;
  private Double avgLocalRate;
  private Queue<Double> localRates;
  private Double avgRemoteRate;
  private Queue<Double> remoteRates;
  private Double avgSelectivity;
  private Queue<Double> selectivities;

  public DetailedMapPredictionStats(int maxHistory, int relevance) {
    super(maxHistory, relevance);
    this.rates = new ArrayBlockingQueue<>(maxHistory);
    this.localRates = new ArrayBlockingQueue<>(maxHistory);
    this.remoteRates = new ArrayBlockingQueue<>(maxHistory);
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

  public Double getAvgLocalRate() {
    return avgLocalRate;
  }

  public void setAvgLocalRate(Double avgLocalRate) {
    this.avgLocalRate = avgLocalRate;
  }

  public Queue<Double> getLocalRates() {
    return localRates;
  }

  public Double getAvgRemoteRate() {
    return avgRemoteRate;
  }

  public void setAvgRemoteRate(Double avgRemoteRate) {
    this.avgRemoteRate = avgRemoteRate;
  }

  public Queue<Double> getRemoteRates() {
    return remoteRates;
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

  protected void updateStatsFromFlexFields(Map<String, String> flexFields) {
    avgRate = addValue(
      flexFields.get(FLEX_KEY_PREFIX + MAP_GENERAL),
      avgRate,
      rates
    );
    avgLocalRate = addValue(
      flexFields.get(FLEX_KEY_PREFIX + MAP_LOCAL),
      avgLocalRate,
      localRates
    );
    avgRemoteRate = addValue(
      flexFields.get(FLEX_KEY_PREFIX + MAP_REMOTE),
      avgRemoteRate,
      remoteRates
    );
    avgSelectivity = addValue(
      flexFields.get(FLEX_KEY_PREFIX + MAP_SELECTIVITY),
      avgSelectivity,
      selectivities
    );
  }

  protected void addSource(JobProfile job) {
    updateStatsFromFlexFields(job.getFlexFields());
    sampleSize++;
  }

  @Override
  protected boolean isIncomplete() {
    return avgLocalRate == null ||
      avgRemoteRate==null ||
      avgSelectivity == null ||
      avgRate == null;
  }

}
