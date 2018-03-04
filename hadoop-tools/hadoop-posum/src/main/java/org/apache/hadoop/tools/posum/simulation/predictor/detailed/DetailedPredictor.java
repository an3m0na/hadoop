package org.apache.hadoop.tools.posum.simulation.predictor.detailed;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.TaskProfile;
import org.apache.hadoop.tools.posum.simulation.predictor.TaskPredictionInput;
import org.apache.hadoop.tools.posum.simulation.predictor.TaskPredictionOutput;
import org.apache.hadoop.tools.posum.simulation.predictor.simple.SimpleRateBasedPredictor;
import org.apache.hadoop.tools.posum.simulation.predictor.stats.RegressionWithFallbackStatEntry;

import java.util.List;

import static org.apache.hadoop.tools.posum.common.util.GeneralUtils.orZero;
import static org.apache.hadoop.tools.posum.common.util.cluster.ClusterUtils.getSplitSize;
import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.DetailedStatKeys.MAP_LOCAL_DURATION;
import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.DetailedStatKeys.MAP_REMOTE_DURATION;
import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.DetailedStatKeys.MAP_SELECTIVITY;
import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.DetailedStatKeys.MERGE_DURATION;
import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.DetailedStatKeys.REDUCE_ONLY_DURATION;
import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.DetailedStatKeys.SHUFFLE_FIRST_DURATION;
import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.DetailedStatKeys.SHUFFLE_TYPICAL_DURATION;
import static org.apache.hadoop.tools.posum.simulation.predictor.simple.SimpleStatKeys.MAP_DURATION;
import static org.apache.hadoop.tools.posum.simulation.predictor.simple.SimpleStatKeys.REDUCE_DURATION;

public class DetailedPredictor extends SimpleRateBasedPredictor<DetailedPredictionModel, DetailedPredictionProfile> {

  private static final Log logger = LogFactory.getLog(DetailedPredictor.class);

  public DetailedPredictor(Configuration conf) {
    super(conf);
  }

  @Override
  protected DetailedPredictionModel initializeModel() {
    return new DetailedPredictionModel(historyBuffer);
  }

  @Override
  protected DetailedPredictionProfile buildPredictionProfile(JobProfile job) {
    DetailedMapPredictionStats mapStats = new DetailedMapPredictionStats(0);
    DetailedReducePredictionStats reduceStats = new DetailedReducePredictionStats(0);
    DetailedPredictionProfile predictionProfile = new DetailedPredictionProfile(job, mapStats, reduceStats);
    predictionProfile.deserialize();

    List<TaskProfile> tasks = null;
    if (mapStats.getSampleSize(MAP_DURATION) != job.getCompletedMaps()) { // new information is available
      tasks = getJobTasks(job.getId(), orZero(job.getFinishTime()) != 0);
      mapStats.addSamples(job, tasks);
      predictionProfile.markUpdated();
    }

    if (reduceStats.getSampleSize(REDUCE_DURATION) != job.getCompletedReduces()) { // new information is available
      if (tasks == null)
        tasks = getJobTasks(job.getId(), orZero(job.getFinishTime()) != 0);
      reduceStats.addSamples(job, tasks);
      predictionProfile.markUpdated();
    }

    return predictionProfile;
  }

  @Override
  protected TaskPredictionOutput predictMapTaskBehavior(TaskPredictionInput input, DetailedPredictionProfile predictionProfile) {
    JobProfile job = input.getJob();
    DetailedMapPredictionStats historicalStats = model.getRelevantMapStats(job);
    DetailedMapPredictionStats jobStats = predictionProfile.getMapStats();

    Boolean local = null;
    if (input.getNodeAddress() != null && input.getTask() != null)
      local = input.getTask().getSplitLocations().contains(input.getNodeAddress());
    else {
      if (input instanceof DetailedTaskPredictionInput)
        local = ((DetailedTaskPredictionInput) input).getLocal();
    }

    Long taskInput = getSplitSize(input.getTask(), job);

    RegressionWithFallbackStatEntry durationStatEntry = null;
    if (local != null)
      // we know locality; try to find relevant map rate w.r.t locality
      durationStatEntry = getRelevantStat(local ? MAP_LOCAL_DURATION : MAP_REMOTE_DURATION, historicalStats, jobStats);
    if (durationStatEntry == null) {
      // try to find any map rate
      durationStatEntry = getAnyStat(MAP_DURATION, historicalStats, jobStats);
    }
    if (durationStatEntry == null || taskInput == null) {
      return handleNoMapRateInfo(job, historicalStats, jobStats);
    }

    Long duration = predictMapFromStat(taskInput.doubleValue(), durationStatEntry);
    if (duration == null) {
      return handleNoMapRateInfo(job, historicalStats, jobStats);
    }

    return new TaskPredictionOutput(duration);
  }

  private static Long predictMapFromStat(Double taskInput, RegressionWithFallbackStatEntry statEntry) {
    if (taskInput == null || statEntry == null)
      return null;
    // restrict to a minimum of 1 byte per task to avoid multiplication or division by zero
    Double prediction = statEntry.predict(Math.max(taskInput, 1.0));
    return prediction == null ? null : prediction.longValue();
  }

  @Override
  protected TaskPredictionOutput predictReduceTaskBehavior(TaskPredictionInput input, DetailedPredictionProfile predictionProfile) {
    JobProfile job = input.getJob();
    DetailedReducePredictionStats historicalStats = model.getRelevantReduceStats(job);
    DetailedReducePredictionStats jobStats = predictionProfile.getReduceStats();

    DetailedMapPredictionStats historicalMapStats = model.getRelevantMapStats(job);
    DetailedMapPredictionStats jobMapStats = predictionProfile.getMapStats();
    Double avgSelectivity = getRelevantAverage(MAP_SELECTIVITY, historicalMapStats, jobMapStats);

    // try a detailed prediction using phase-specific statistics
    Long duration = predictReduceByPhases(job, avgSelectivity, historicalStats, jobStats);
    if (duration != null)
      return new TaskPredictionOutput(duration);

    Double avgReduceDuration = getRelevantAverage(REDUCE_DURATION, historicalStats, jobStats);
    if (avgReduceDuration != null)
      return new TaskPredictionOutput(avgReduceDuration.longValue());

    // assume reduce rate is equal to map rate
    RegressionWithFallbackStatEntry mapDurationStatEntry = getAnyStat(MAP_DURATION, historicalMapStats, jobMapStats);
    avgSelectivity = getAnyAverage(MAP_SELECTIVITY, historicalMapStats, jobMapStats);
    logger.trace("Detailed reduce prediction was not possible for " + job.getId() + ". Trying map stat and any selectivity " + avgSelectivity);

    // calculate how much input the task should have based on how much is left and how many reduces remain
    Double inputPerTask = calculateInputPerReduce(job, avgSelectivity);
    // calculate average duration based map processing rate
    duration = predictMapFromStat(inputPerTask, mapDurationStatEntry);
    if (duration == null)
      return handleNoReduceInfo(job);
    return new TaskPredictionOutput(duration);
  }

  private Long predictReduceByPhases(JobProfile job,
                                     Double avgSelectivity,
                                     DetailedReducePredictionStats historicalStats,
                                     DetailedReducePredictionStats jobStats) {
    if (avgSelectivity == null)
      return null;
    // calculate how much input the task should have based on how much is left and how many reduces remain
    Double inputPerTask = calculateInputPerReduce(job, avgSelectivity);
    Long shuffleTime = predictShuffleTime(job, historicalStats, jobStats, inputPerTask);
    if (shuffleTime == null)
      return null;
    Long mergeTime = predictTimeByStat(MERGE_DURATION, historicalStats, jobStats, inputPerTask);
    if (mergeTime == null)
      return null;
    Long reduceTime = predictTimeByStat(REDUCE_ONLY_DURATION, historicalStats, jobStats, inputPerTask);
    if (reduceTime == null)
      return null;
    Long duration = shuffleTime + mergeTime + reduceTime;
    logger.trace("Detailed reduce duration for " + job.getId() + " should be " + shuffleTime + " + " + mergeTime + " + " + reduceTime + "=" + duration);
    return duration;
  }

  private Long predictShuffleTime(JobProfile job,
                                  DetailedReducePredictionStats historicalStats,
                                  DetailedReducePredictionStats jobStats,
                                  Double inputPerTask) {
    RegressionWithFallbackStatEntry firstShuffleStatEntry = getRelevantStat(SHUFFLE_FIRST_DURATION, historicalStats, jobStats);
    if (!job.getTotalMapTasks().equals(job.getCompletedMaps()) && firstShuffleStatEntry != null) // predictable first shuffle
      return firstShuffleStatEntry.getAverage().longValue();
    return predictTimeByStat(SHUFFLE_TYPICAL_DURATION, historicalStats, jobStats, inputPerTask);
  }

  private Long predictTimeByStat(Enum key,
                                 DetailedReducePredictionStats historicalStats,
                                 DetailedReducePredictionStats jobStats,
                                 Double inputPerTask) {
    RegressionWithFallbackStatEntry statEntry = getRelevantStat(key, historicalStats, jobStats);
    if (statEntry == null)
      return null;
    Double prediction = statEntry.predict(inputPerTask);
    return prediction == null ? null : prediction.longValue();
  }
}
