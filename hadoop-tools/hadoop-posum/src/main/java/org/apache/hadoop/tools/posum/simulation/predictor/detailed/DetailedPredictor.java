package org.apache.hadoop.tools.posum.simulation.predictor.detailed;

import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.TaskProfile;
import org.apache.hadoop.tools.posum.common.util.PosumException;
import org.apache.hadoop.tools.posum.simulation.predictor.TaskPredictionInput;
import org.apache.hadoop.tools.posum.simulation.predictor.TaskPredictionOutput;
import org.apache.hadoop.tools.posum.simulation.predictor.simple.SimpleRateBasedPredictor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.tools.posum.common.util.GeneralUtils.orZero;
import static org.apache.hadoop.tools.posum.common.util.cluster.ClusterUtils.getDoubleField;
import static org.apache.hadoop.tools.posum.common.util.cluster.ClusterUtils.getDuration;
import static org.apache.hadoop.tools.posum.common.util.cluster.ClusterUtils.getIntField;
import static org.apache.hadoop.tools.posum.common.util.cluster.ClusterUtils.getLongField;
import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.FlexKeys.MAP_FINISH;
import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.FlexKeys.MAP_GENERAL;
import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.FlexKeys.MAP_LOCAL;
import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.FlexKeys.MAP_REMOTE;
import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.FlexKeys.MAP_SELECTIVITY;
import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.FlexKeys.MERGE;
import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.FlexKeys.PROFILED_MAPS;
import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.FlexKeys.PROFILED_REDUCES;
import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.FlexKeys.REDUCE;
import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.FlexKeys.SHUFFLE_FIRST;
import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.FlexKeys.SHUFFLE_TYPICAL;

public class DetailedPredictor extends SimpleRateBasedPredictor<DetailedPredictionModel> {

  private static final Log logger = LogFactory.getLog(DetailedPredictor.class);

  public DetailedPredictor(Configuration conf) {
    super(conf);
  }

  @Override
  protected DetailedPredictionModel initializeModel() {
    return new DetailedPredictionModel(historyBuffer);
  }

  @Override
  protected Map<String, String> getPredictionProfileUpdates(JobProfile job, boolean fromHistory) {
    Map<String, String> fieldMap = new HashMap<>(FlexKeys.values().length);
    long mapFinish = 0L; // keeps track of the finish time of the last map task
    double mapRate = 0.0, mapRemoteRate = 0.0, mapLocalRate = 0.0, shuffleTypicalRate = 0.0, mergeRate = 0.0, reduceRate = 0.0;
    int totalMaps = 0, mapRemoteNo = 0, mapLocalNo = 0, typicalShuffleNo = 0, firstShuffleNo = 0, reduceNo = 0;
    long shuffleFirstTime = 0L;
    List<TaskProfile> tasks = null;

    if (!getIntField(job, PROFILED_MAPS.getKey(), 0).equals(job.getCompletedMaps())) {
      // nothing will work if we don't have input size info
      if (job.getTotalSplitSize() != null) {
        Long parsedInputBytes = 0L;

        tasks = getJobTasks(job.getId(), fromHistory);
        if (tasks == null)
          throw new PosumException("Tasks not found or finished for job " + job.getId());
        for (TaskProfile task : tasks) {
          if (getDuration(task) <= 0 || !task.getType().equals(TaskType.MAP))
            continue;
          totalMaps++;
          // this is a finished map task; calculate general, local and remote processing rates
          Long taskInput = getSplitSize(task, job);
          parsedInputBytes += taskInput;
          if (mapFinish < task.getFinishTime())
            mapFinish = task.getFinishTime();
          // restrict to a minimum of 1 byte per task to avoid multiplication or division by zero
          Double newRate = 1.0 * taskInput / getDuration(task);
          mapRate += newRate;
          if (Boolean.TRUE.equals(task.isLocal())) { // because protos are unpredictable
            mapLocalRate += newRate;
            mapLocalNo++;
          } else {
            mapRemoteRate += newRate;
            mapRemoteNo++;
          }
        }
        if (totalMaps != 0) {
          fieldMap.put(MAP_GENERAL.getKey(), Double.toString(mapRate / totalMaps));
          if (mapLocalNo != 0 && mapLocalRate != 0) {
            fieldMap.put(MAP_LOCAL.getKey(), Double.toString(mapLocalRate / mapLocalNo));
          }
          if (mapRemoteNo != 0 && mapRemoteRate != 0) {
            fieldMap.put(MAP_REMOTE.getKey(), Double.toString(mapRemoteRate / mapRemoteNo));
          }
          if (job.getMapOutputBytes() != null) {
            fieldMap.put(MAP_SELECTIVITY.getKey(),
              // restrict to a minimum of 1 byte per task to avoid multiplication or division by zero
              Double.toString(1.0 * orZero(job.getMapOutputBytes()) / parsedInputBytes));
          }
          if (totalMaps == job.getTotalMapTasks()) {
            // all map tasks were parsed
            fieldMap.put(MAP_FINISH.getKey(), Long.toString(mapFinish));
          }
        }
        fieldMap.put(PROFILED_MAPS.getKey(), Integer.toString(totalMaps));
      }
    }

    if (!getIntField(job, PROFILED_REDUCES.getKey(), 0).equals(job.getCompletedReduces())) {
      if (tasks == null)
        tasks = getJobTasks(job.getId(), fromHistory);
      if (tasks == null)
        throw new PosumException("Tasks not found or finished for job " + job.getId());

      if (mapFinish >= Long.MAX_VALUE) {
        // mapFinish might not have been initialized
        mapFinish = getLongField(job, MAP_FINISH.getKey(), Long.MAX_VALUE);
      }
      for (TaskProfile task : tasks) {
        if (getDuration(task) <= 0 || !task.getType().equals(TaskType.REDUCE) || task.getInputBytes() == null)
          continue;
        reduceNo++;
        // this is a finished reduce task; split stats into shuffle, merge and reduce
        Long taskInputBytes = Math.max(orZero(task.getInputBytes()), 1);
        if (orZero(task.getReduceTime()) > 0)
          reduceRate += 1.0 * taskInputBytes / task.getReduceTime();
        if (orZero(task.getMergeTime()) > 0)
          mergeRate += 1.0 * taskInputBytes / task.getMergeTime();
        if (task.getStartTime() >= mapFinish) {
          // the task was not in the first reduce wave; store shuffle time under typical
          shuffleTypicalRate += 1.0 * taskInputBytes / task.getShuffleTime();
          typicalShuffleNo++;
        } else {
          shuffleFirstTime += task.getStartTime() + orZero(task.getShuffleTime()) - mapFinish;
          firstShuffleNo++;
        }
      }
      if (reduceNo > 0) {
        fieldMap.put(REDUCE.getKey(), Double.toString(reduceRate / reduceNo));
        fieldMap.put(MERGE.getKey(), Double.toString(mergeRate / reduceNo));
        if (shuffleFirstTime != 0) {
          fieldMap.put(SHUFFLE_FIRST.getKey(),
            Long.toString(shuffleFirstTime / firstShuffleNo));
        }
        if (shuffleTypicalRate != 0) {
          fieldMap.put(SHUFFLE_TYPICAL.getKey(),
            Double.toString(shuffleTypicalRate / typicalShuffleNo));
        }
        fieldMap.put(PROFILED_REDUCES.getKey(), Integer.toString(reduceNo));
      }
    }
    return fieldMap;
  }

  @Override
  protected TaskPredictionOutput predictMapTaskBehavior(TaskPredictionInput input) {
    JobProfile job = input.getJob();
    DetailedMapPredictionStats jobStats = new DetailedMapPredictionStats(1, 0);
    jobStats.addSource(job);

    Boolean local = null;
    if (input.getNodeAddress() != null)
      local = input.getTask().getSplitLocations().contains(input.getNodeAddress());
    else {
      if (input instanceof DetailedTaskPredictionInput)
        local = ((DetailedTaskPredictionInput) input).getLocal();
    }

    Double rate = local == null ? jobStats.getAvgRate() : local ? jobStats.getAvgLocalRate() : jobStats.getAvgRemoteRate();

    if (rate == null) {
      // we don't know the rate of that type
      // get the appropriate average map processing rate from history
      DetailedMapPredictionStats mapStats = model.getRelevantMapStats(job);
      if (mapStats == null)
        return handleNoMapInfo(job);
      if (mapStats.getRelevance() > 1 && job.getAvgMapDuration() != null)
        // if history is not relevant and we have the current average duration, return it
        return new TaskPredictionOutput(job.getAvgMapDuration());
      rate = local == null ? mapStats.getAvgRate() : local ? mapStats.getAvgLocalRate() : mapStats.getAvgRemoteRate();
      if (rate == null)
        return handleNoMapInfo(job);
    }
    // multiply by how much input each task has
    Long splitSize = getSplitSize(input.getTask(), job);
    if (splitSize == null)
      return handleNoMapInfo(job);
    Double duration = splitSize / rate;
    logger.trace("Map duration for " + job.getId() + " should be " + splitSize + " / " + rate + "=" + duration);
    return new TaskPredictionOutput(duration.longValue());
  }

  @Override
  protected TaskPredictionOutput handleNoMapInfo(JobProfile job) {
    if (job.getAvgMapDuration() != null)
      // if we do have at least the current average duration, return that, regardless of location
      return new TaskPredictionOutput(job.getAvgMapDuration());
    return super.handleNoMapInfo(job);
  }

  @Override
  protected TaskPredictionOutput predictReduceTaskBehavior(TaskPredictionInput input) {
    JobProfile job = input.getJob();
    Double avgSelectivity = getMapTaskSelectivity(
      job,
      model.getRelevantMapStats(job),
      MAP_SELECTIVITY.getKey()
    );

    DetailedReducePredictionStats jobStats = new DetailedReducePredictionStats(1, 0);
    jobStats.addSource(job);

    if (jobStats.isIncomplete()) {
      // we are missing information; get averages from history to compensate
      DetailedReducePredictionStats reduceStats = model.getRelevantReduceStats(job);
      if (reduceStats != null) {
        jobStats.completeFrom(reduceStats);
      }
    }

    // try a detailed prediction using phase-specific statistics
    TaskPredictionOutput duration = predictReduceByPhases(job, avgSelectivity, jobStats);
    if (duration != null)
      // prediction was possible
      return duration;

    if (jobStats.getAvgReduceDuration() == null)
      // we have no current or historical reduce information, not even average duration
      return handleNoReduceInfo(job, avgSelectivity, getDoubleField(job, MAP_GENERAL.getKey(), null));

    // we are still missing information
    // just return average reduce duration of the current job or historical jobs
    logger.trace("Reduce duration calculated as simple average for " + job.getId() + " =  " + jobStats.getAvgReduceDuration());
    return new TaskPredictionOutput(jobStats.getAvgReduceDuration().longValue());
  }

  private TaskPredictionOutput predictReduceByPhases(JobProfile job, Double avgSelectivity, DetailedReducePredictionStats jobStats) {
    if (avgSelectivity == null)
      return null;
    // calculate how much input the task should have based on how much is left and how many reduces remain
    // restrict to a minimum of 1 byte per task to avoid multiplication or division by zero
    Double inputLeft = orZero(job.getTotalSplitSize()) * avgSelectivity - orZero(job.getReduceInputBytes());
    Double inputPerTask = Math.max(inputLeft / (job.getTotalReduceTasks() - job.getCompletedReduces()), 1);
    Long shuffleTime =
      predictShuffleTime(jobStats, ObjectUtils.equals(job.getCompletedMaps(), job.getTotalMapTasks()), inputPerTask);
    if (shuffleTime == null)
      return null;
    Double duration = shuffleTime + inputPerTask / jobStats.getAvgMergeRate() + inputPerTask / jobStats.getAvgReduceRate();
    logger.trace("Reduce duration for " + job.getId() + " should be " + shuffleTime + " + " +
      inputPerTask + " / " + jobStats.getAvgMergeRate() + " + " +
      inputPerTask + " / " + jobStats.getAvgReduceRate() + "=" + duration);
    return new TaskPredictionOutput(duration.longValue());
  }

  private Long predictShuffleTime(DetailedReducePredictionStats jobStats, boolean isFirstShuffle, Double inputPerTask) {
    if (isFirstShuffle && jobStats.getAvgShuffleFirstTime() != null)
      return jobStats.getAvgShuffleFirstTime().longValue();

    if (jobStats.getAvgShuffleTypicalRate() == null)
      return null;
    return Double.valueOf(inputPerTask / jobStats.getAvgShuffleTypicalRate()).longValue();
  }
}
