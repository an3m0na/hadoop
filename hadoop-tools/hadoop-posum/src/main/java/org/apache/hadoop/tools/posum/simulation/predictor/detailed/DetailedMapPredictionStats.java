package org.apache.hadoop.tools.posum.simulation.predictor.detailed;

import org.apache.hadoop.tools.posum.simulation.predictor.simple.SimpleMapPredictionStats;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.FlexKeys.MAP_GENERAL;
import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.FlexKeys.MAP_LOCAL;
import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.FlexKeys.MAP_REMOTE;
import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.FlexKeys.MAP_SELECTIVITY;

class DetailedMapPredictionStats extends SimpleMapPredictionStats {
  private Double avgLocalRate;
  private Queue<Double> localRates;
  private Double avgRemoteRate;
  private Queue<Double> remoteRates;

  public DetailedMapPredictionStats(int maxHistory, int relevance) {
    super(maxHistory, relevance, MAP_GENERAL.getKey(), MAP_SELECTIVITY.getKey());
    this.localRates = new ArrayBlockingQueue<>(maxHistory);
    this.remoteRates = new ArrayBlockingQueue<>(maxHistory);
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

  @Override
  public void updateStatsFromFlexFields(Map<String, String> flexFields) {
    super.updateStatsFromFlexFields(flexFields);
    avgLocalRate = addValue(
      flexFields.get(MAP_LOCAL.getKey()),
      avgLocalRate,
      localRates
    );
    avgRemoteRate = addValue(
      flexFields.get(MAP_REMOTE.getKey()),
      avgRemoteRate,
      remoteRates
    );
  }
}
