package org.apache.hadoop.tools.posum.simulation.predictor.detailed;

import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.simulation.predictor.PredictionStats;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.FlexKeys.MERGE;
import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.FlexKeys.REDUCE;
import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.FlexKeys.SHUFFLE_FIRST;
import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.FlexKeys.SHUFFLE_TYPICAL;

class DetailedReducePredictionStats extends PredictionStats {
  private Double avgReduceDuration;
  private Queue<Double> reduceDurations;
  private Double avgShuffleTypicalRate;
  private Queue<Double> shuffleTypicalRates;
  private Double avgShuffleFirstTime;
  private Queue<Double> shuffleFirstTimes;
  private Double avgMergeRate;
  private Queue<Double> mergeRates;
  private Double avgReduceRate;
  private Queue<Double> reduceRates;

  public DetailedReducePredictionStats(int maxHistory, int relevance) {
    super(maxHistory, relevance);
    this.reduceDurations = new ArrayBlockingQueue<>(maxHistory);
    this.shuffleTypicalRates = new ArrayBlockingQueue<>(maxHistory);
    this.shuffleFirstTimes = new ArrayBlockingQueue<>(maxHistory);
    this.mergeRates = new ArrayBlockingQueue<>(maxHistory);
    this.reduceRates = new ArrayBlockingQueue<>(maxHistory);
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
    avgShuffleTypicalRate = addValue(
      flexFields.get(SHUFFLE_TYPICAL.getKey()),
      avgShuffleTypicalRate,
      shuffleTypicalRates
    );
    avgShuffleFirstTime = addValue(
      flexFields.get(SHUFFLE_FIRST.getKey()),
      avgShuffleFirstTime,
      shuffleFirstTimes
    );
    avgMergeRate = addValue(
      flexFields.get(MERGE.getKey()),
      avgMergeRate,
      mergeRates
    );
    avgReduceRate = addValue(
      flexFields.get(REDUCE.getKey()),
      avgReduceRate,
      reduceRates
    );
  }

  public void addSource(JobProfile job) {
    updateStatsFromFlexFields(job.getFlexFields());
    if (job.getAvgReduceDuration() != null) {
      avgReduceDuration = addValue(job.getAvgReduceDuration().doubleValue(), avgReduceDuration, reduceDurations);
    }
    incrementSampleSize();
  }

  public boolean isIncomplete() {
    return avgReduceDuration == null ||
      avgShuffleTypicalRate == null ||
      avgShuffleFirstTime == null ||
      avgMergeRate == null ||
      avgReduceRate == null;
  }

  void completeFrom(DetailedReducePredictionStats otherStats) {
    if (avgReduceDuration == null && otherStats.getAvgReduceDuration() != null)
      avgReduceDuration = otherStats.getAvgReduceDuration();
    if (avgShuffleTypicalRate == null && otherStats.getAvgShuffleTypicalRate() != null)
      avgShuffleTypicalRate = otherStats.getAvgShuffleTypicalRate();
    if (avgShuffleFirstTime == null && otherStats.getAvgShuffleFirstTime() != null)
      avgShuffleFirstTime = otherStats.getAvgShuffleFirstTime();
    if (avgMergeRate == null && otherStats.getAvgMergeRate() != null)
      avgMergeRate = otherStats.getAvgMergeRate();
    if (avgReduceRate == null && otherStats.getAvgReduceRate() != null)
      avgReduceRate = otherStats.getAvgReduceRate();
    setRelevance(otherStats.getRelevance());
  }
}
