package org.apache.hadoop.tools.posum.simulation.predictor.detailed;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.TaskProfile;
import org.apache.hadoop.tools.posum.common.util.PosumException;
import org.apache.hadoop.tools.posum.simulation.predictor.TaskPredictionInput;
import org.apache.hadoop.tools.posum.simulation.predictor.TaskPredictionOutput;
import org.apache.hadoop.tools.posum.simulation.predictor.simple.SimpleRateBasedPredictor;

import java.util.List;

import static org.apache.hadoop.tools.posum.common.util.cluster.ClusterUtils.getSplitSize;
import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.DetailedStatKeys.MAP_LOCAL_RATE;
import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.DetailedStatKeys.MAP_RATE;
import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.DetailedStatKeys.MAP_REMOTE_RATE;
import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.DetailedStatKeys.MAP_SELECTIVITY;
import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.DetailedStatKeys.MERGE_RATE;
import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.DetailedStatKeys.REDUCE_RATE;
import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.DetailedStatKeys.SHUFFLE_FIRST_DURATION;
import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.DetailedStatKeys.SHUFFLE_TYPICAL_RATE;
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

    List<TaskProfile> tasks = getJobTasks(job.getId(), job.getFinishTime() != null);
    if (tasks == null)
      throw new PosumException("Tasks not found or finished for job " + job.getId());

    if (mapStats.getSampleSize(MAP_DURATION) != job.getCompletedMaps()) { // new information is available
      mapStats.addSamples(job, tasks);
    }

    if (reduceStats.getSampleSize(REDUCE_DURATION) != job.getCompletedReduces()) { // new information is available
      reduceStats.addSamples(job, tasks);
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
    Double avgRate = null;
    if (local != null)
      // we know locality; try to find relevant map rate w.r.t locality
      avgRate = getRelevantStat(local ? MAP_LOCAL_RATE : MAP_REMOTE_RATE, historicalStats, jobStats);
    if (avgRate == null) {
      // try to find any map rate
      avgRate = getAnyStat(MAP_RATE, historicalStats, jobStats);
    }
    if (avgRate == null || taskInput == null)
      return handleNoMapRateInfo(job, historicalStats, jobStats);

    Long duration = predictMapByRate(job, taskInput, avgRate);
    if (duration == null)
      return handleNoMapRateInfo(job, historicalStats, jobStats);
    return new TaskPredictionOutput(duration);
  }

  @Override
  protected TaskPredictionOutput predictReduceTaskBehavior(TaskPredictionInput input, DetailedPredictionProfile predictionProfile) {
    JobProfile job = input.getJob();
    DetailedReducePredictionStats historicalStats = model.getRelevantReduceStats(job);
    DetailedReducePredictionStats jobStats = predictionProfile.getReduceStats();

    DetailedMapPredictionStats historicalMapStats = model.getRelevantMapStats(job);
    DetailedMapPredictionStats jobMapStats = predictionProfile.getMapStats();
    Double avgSelectivity = getRelevantStat(MAP_SELECTIVITY, historicalMapStats, jobMapStats);

    // try a detailed prediction using phase-specific statistics
    Long duration = predictReduceByPhases(job, avgSelectivity, historicalStats, jobStats);
    if (duration != null)
      return new TaskPredictionOutput(duration);

    Double avgReduceDuration = getRelevantStat(REDUCE_DURATION, historicalStats, jobStats);
    if (avgReduceDuration != null)
      return new TaskPredictionOutput(avgReduceDuration.longValue());

    // assume reduce rate is equal to map rate
    Double avgRate = getAnyStat(MAP_RATE, historicalMapStats, jobMapStats);
    // get any historical selectivity
    avgSelectivity = getAnyStat(MAP_SELECTIVITY, historicalMapStats, jobMapStats);
    logger.trace("Detailed reduce prediction was not possible for " + job.getId() + ". Trying map rate " + avgRate + " and any selectivity " + avgSelectivity);

    // calculate average duration based on map selectivity and processing rate
    duration = predictReduceByRate(job, avgSelectivity, avgRate);
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
    // restrict to a minimum of 1 byte per task to avoid multiplication or division by zero
    Double inputPerTask = calculateInputPerReduce(job, avgSelectivity);
    Long shuffleTime = predictShuffleTime(job, historicalStats, jobStats, inputPerTask);
    if (shuffleTime == null)
      return null;
    Long mergeTime = predictTimeByRate(job, MERGE_RATE, historicalStats, jobStats, inputPerTask);
    if (mergeTime == null)
      return null;
    Long reduceTime = predictTimeByRate(job, REDUCE_RATE, historicalStats, jobStats, inputPerTask);
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
    Double firstShuffleDuration = getRelevantStat(SHUFFLE_FIRST_DURATION, historicalStats, jobStats);
    if (!job.getTotalMapTasks().equals(job.getCompletedMaps()) && firstShuffleDuration != null) // predictable first shuffle
      return firstShuffleDuration.longValue();
    return predictTimeByRate(job, SHUFFLE_TYPICAL_RATE, historicalStats, jobStats, inputPerTask);
  }

  private Long predictTimeByRate(JobProfile job,
                                 Enum key,
                                 DetailedReducePredictionStats historicalStats,
                                 DetailedReducePredictionStats jobStats,
                                 Double inputPerTask) {
    Double rate = getRelevantStat(key, historicalStats, jobStats);
    if (rate == null)
      return null;
    Double duration = inputPerTask / rate;
    logger.trace(key + " duration for " + job.getId() + " should be " + inputPerTask + " / " + rate + "=" + duration);
    return duration.longValue();
  }
}
