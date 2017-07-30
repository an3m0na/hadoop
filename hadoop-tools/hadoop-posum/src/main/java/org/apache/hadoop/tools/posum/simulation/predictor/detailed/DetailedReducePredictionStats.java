package org.apache.hadoop.tools.posum.simulation.predictor.detailed;

import org.apache.hadoop.tools.posum.simulation.predictor.simple.SimpleReducePredictionStats;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.FlexKeys.MERGE;
import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.FlexKeys.REDUCE;
import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.FlexKeys.SHUFFLE_FIRST;
import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.FlexKeys.SHUFFLE_TYPICAL;

class DetailedReducePredictionStats extends SimpleReducePredictionStats {
  private Double avgShuffleTypicalRate;
  private Queue<Double> shuffleTypicalRates;
  private Double avgShuffleFirstTime;
  private Queue<Double> shuffleFirstTimes;
  private Double avgMergeRate;
  private Queue<Double> mergeRates;

  public DetailedReducePredictionStats(int maxHistory, int relevance) {
    super(maxHistory, relevance, REDUCE.getKey());
    this.shuffleTypicalRates = new ArrayBlockingQueue<>(maxHistory);
    this.shuffleFirstTimes = new ArrayBlockingQueue<>(maxHistory);
    this.mergeRates = new ArrayBlockingQueue<>(maxHistory);
  }

  public Double getAvgShuffleTypicalRate() {
    return avgShuffleTypicalRate;
  }

  public void setAvgShuffleTypicalRate(Double avgShuffleTypicalRate) {
    this.avgShuffleTypicalRate = avgShuffleTypicalRate;
  }

  public Queue<Double> getShuffleTypicalRates() {
    return shuffleTypicalRates;
  }

  public Double getAvgShuffleFirstTime() {
    return avgShuffleFirstTime;
  }

  public void setAvgShuffleFirstTime(Double avgShuffleFirstTime) {
    this.avgShuffleFirstTime = avgShuffleFirstTime;
  }

  public Queue<Double> getShuffleFirstTimes() {
    return shuffleFirstTimes;
  }

  public Double getAvgMergeRate() {
    return avgMergeRate;
  }

  public void setAvgMergeRate(Double avgMergeRate) {
    this.avgMergeRate = avgMergeRate;
  }

  public Queue<Double> getMergeRates() {
    return mergeRates;
  }

  public void updateStatsFromFlexFields(Map<String, String> flexFields) {
    super.updateStatsFromFlexFields(flexFields);
    avgShuffleTypicalRate = addValue(
      flexFields == null ? null : flexFields.get(SHUFFLE_TYPICAL.getKey()),
      avgShuffleTypicalRate,
      shuffleTypicalRates
    );
    avgShuffleFirstTime = addValue(
      flexFields == null ? null : flexFields.get(SHUFFLE_FIRST.getKey()),
      avgShuffleFirstTime,
      shuffleFirstTimes
    );
    avgMergeRate = addValue(
      flexFields == null ? null : flexFields.get(MERGE.getKey()),
      avgMergeRate,
      mergeRates
    );
  }

  boolean isIncomplete() {
    return getAvgReduceDuration() == null ||
      getAvgReduceRate() == null ||
      avgShuffleTypicalRate == null ||
      avgShuffleFirstTime == null ||
      avgMergeRate == null;
  }

  void completeFrom(DetailedReducePredictionStats otherStats) {
    if (getAvgReduceDuration() == null && otherStats.getAvgReduceDuration() != null)
      setAvgReduceDuration(otherStats.getAvgReduceDuration());
    if (getAvgReduceRate() == null && otherStats.getAvgReduceRate() != null)
      setAvgReduceRate(otherStats.getAvgReduceRate());
    setRelevance(otherStats.getRelevance());
    if (avgShuffleTypicalRate == null && otherStats.getAvgShuffleTypicalRate() != null)
      avgShuffleTypicalRate = otherStats.getAvgShuffleTypicalRate();
    if (avgShuffleFirstTime == null && otherStats.getAvgShuffleFirstTime() != null)
      avgShuffleFirstTime = otherStats.getAvgShuffleFirstTime();
    if (avgMergeRate == null && otherStats.getAvgMergeRate() != null)
      avgMergeRate = otherStats.getAvgMergeRate();
  }
}
