package org.apache.hadoop.tools.posum.simulation.predictor.detailed;

import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.tools.posum.client.data.Database;
import org.apache.hadoop.tools.posum.common.records.call.FindByIdCall;
import org.apache.hadoop.tools.posum.common.records.call.FindByQueryCall;
import org.apache.hadoop.tools.posum.common.records.call.IdsByQueryCall;
import org.apache.hadoop.tools.posum.common.records.call.SaveJobFlexFieldsCall;
import org.apache.hadoop.tools.posum.common.records.call.query.QueryUtils;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.TaskProfile;
import org.apache.hadoop.tools.posum.common.util.PosumException;
import org.apache.hadoop.tools.posum.simulation.predictor.JobBehaviorPredictor;
import org.apache.hadoop.tools.posum.simulation.predictor.JobPredictionInput;
import org.apache.hadoop.tools.posum.simulation.predictor.JobPredictionOutput;
import org.apache.hadoop.tools.posum.simulation.predictor.TaskPredictionInput;
import org.apache.hadoop.tools.posum.simulation.predictor.TaskPredictionOutput;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.TASK;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.TASK_HISTORY;
import static org.apache.hadoop.tools.posum.common.util.Utils.getDuration;
import static org.apache.hadoop.tools.posum.common.util.Utils.getIntFieldOrZero;
import static org.apache.hadoop.tools.posum.common.util.Utils.getSplitSize;
import static org.apache.hadoop.tools.posum.common.util.Utils.orZero;
import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.Common.FLEX_KEY_PREFIX;
import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.Common.FlexKeys.MAP_GENERAL;
import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.Common.FlexKeys.MAP_LOCAL;
import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.Common.FlexKeys.MAP_REMOTE;
import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.Common.FlexKeys.MAP_SELECTIVITY;
import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.Common.FlexKeys.MERGE;
import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.Common.FlexKeys.PROFILED_MAPS;
import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.Common.FlexKeys.PROFILED_REDUCES;
import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.Common.FlexKeys.REDUCE;
import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.Common.FlexKeys.SHUFFLE_FIRST;
import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.Common.FlexKeys.SHUFFLE_TYPICAL;

public class DetailedPredictor extends JobBehaviorPredictor {

  private static final Log logger = LogFactory.getLog(DetailedPredictor.class);

  private DetailedPredictionModel model = new DetailedPredictionModel(historyBuffer);

  public DetailedPredictor(Configuration conf) {
    super(conf);
  }

  @Override
  public void initialize(Database db) {
    super.initialize(db);

    // populate flex-fields for jobs in history
    IdsByQueryCall getFinishedJobIds = IdsByQueryCall.newInstance(DataEntityCollection.JOB_HISTORY, null);
    List<String> historyJobIds = new LinkedList<>(db.execute(getFinishedJobIds).getEntries());
    historyJobIds.removeAll(model.getSourceJobs());
    for (String jobId : historyJobIds) {
      FindByIdCall getJob = FindByIdCall.newInstance(DataEntityCollection.JOB_HISTORY, jobId);
      JobProfile job = getDatabase().execute(getJob).getEntity();
      updateProfile(job, true);
      model.updateModel(job);
    }
  }

  private List<TaskProfile> getJobTasks(String jobId, boolean fromHistory) {
    FindByQueryCall getTasks = FindByQueryCall.newInstance(fromHistory ? TASK_HISTORY : TASK,
      QueryUtils.is("jobId", jobId));
    return getDatabase().execute(getTasks).getEntities();
  }

  private void updateProfile(JobProfile job, boolean fromHistory) {
    //WARNING! the job and tasks stats might not be consistent because they were queried separately

    Map<String, String> fieldMap = new HashMap<>(Common.FlexKeys.values().length);
    Long mapFinish = 0L; // keeps track of the finish time of the last map task
    Double mapRate = 0.0, mapRemoteRate = 0.0, mapLocalRate = 0.0, shuffleTypicalRate = 0.0, mergeRate = 0.0, reduceRate = 0.0;
    Integer mapRemoteNo = 0, mapLocalNo = 0, typicalShuffleNo = 0, firstShuffleNo = 0, reduceNo = 0;
    Long shuffleFirstTime = 0L;
    List<TaskProfile> tasks = null;

    if (getIntFieldOrZero(job, FLEX_KEY_PREFIX + PROFILED_MAPS) != job.getCompletedMaps()) {
      Long parsedInputBytes = 0L;

      tasks = getJobTasks(job.getId(), fromHistory);
      if (tasks == null)
        throw new PosumException("Tasks not found or finished for job " + job.getId());
      for (TaskProfile task : tasks) {
        if (getDuration(task) <= 0 || !task.getType().equals(TaskType.MAP))
          continue;
        // this is a finished map task; calculate general, local and remote processing rates
        Long taskInput = getSplitSize(task, job);
        parsedInputBytes += taskInput;
        if (mapFinish < orZero(task.getFinishTime()))
          mapFinish = orZero(task.getFinishTime());
        // restrict to a minimum of 1 byte per task to avoid multiplication or division by zero
        Double newRate = 1.0 * taskInput / getDuration(task);
        mapRate += newRate;
        if (task.isLocal()) {
          mapLocalRate += newRate;
          mapLocalNo++;
        } else {
          mapRemoteRate += newRate;
          mapRemoteNo++;
        }
      }
      int totalMaps = mapLocalNo + mapRemoteNo;
      if (totalMaps != 0) {
        fieldMap.put(FLEX_KEY_PREFIX + MAP_GENERAL,
          Double.toString(mapRate / totalMaps));
        if (mapLocalNo != 0 && mapLocalRate != 0) {
          fieldMap.put(FLEX_KEY_PREFIX + MAP_LOCAL,
            Double.toString(mapLocalRate / mapLocalNo));
        }
        if (mapRemoteNo != 0 && mapRemoteRate != 0) {
          fieldMap.put(FLEX_KEY_PREFIX + MAP_REMOTE,
            Double.toString(mapRemoteRate / mapRemoteNo));
        }
        if (job.getMapOutputBytes() != null) {
          fieldMap.put(FLEX_KEY_PREFIX + MAP_SELECTIVITY,
            // restrict to a minimum of 1 byte per task to avoid multiplication or division by zero
            Double.toString(1.0 * orZero(job.getMapOutputBytes()) / parsedInputBytes));
        }
        if (totalMaps != job.getTotalMapTasks()) {
          // map phase has not finished yet
          mapFinish = Long.MAX_VALUE;
        }
      }
      fieldMap.put(FLEX_KEY_PREFIX + PROFILED_MAPS, Integer.toString(totalMaps));
    }

    if (getIntFieldOrZero(job, FLEX_KEY_PREFIX + PROFILED_REDUCES) != job.getCompletedReduces()) {
      if (tasks == null)
        tasks = getJobTasks(job.getId(), fromHistory);
      if (tasks == null)
        throw new PosumException("Tasks not found or finished for job " + job.getId());
      for (TaskProfile task : tasks) {
        if (getDuration(task) <= 0 || !task.getType().equals(TaskType.REDUCE))
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
          logger.debug("When this happens, mapFinish is " + mapFinish);
          shuffleFirstTime += task.getStartTime() + orZero(task.getShuffleTime()) - mapFinish;
          firstShuffleNo++;
        }
      }
      if (reduceNo > 0) {
        fieldMap.put(FLEX_KEY_PREFIX + REDUCE, Double.toString(reduceRate / reduceNo));
        fieldMap.put(FLEX_KEY_PREFIX + MERGE, Double.toString(mergeRate / reduceNo));
        if (shuffleFirstTime != 0) {
          fieldMap.put(FLEX_KEY_PREFIX + SHUFFLE_FIRST,
            Long.toString(shuffleFirstTime / firstShuffleNo));
        }
        if (shuffleTypicalRate != 0) {
          fieldMap.put(FLEX_KEY_PREFIX + SHUFFLE_TYPICAL,
            Double.toString(shuffleTypicalRate / typicalShuffleNo));
        }
        fieldMap.put(FLEX_KEY_PREFIX + PROFILED_REDUCES, Integer.toString(reduceNo));
      }
    }

    if (fieldMap.size() > 0) {
      job.getFlexFields().putAll(fieldMap);
      SaveJobFlexFieldsCall saveFlexFields = SaveJobFlexFieldsCall.newInstance(job.getId(), fieldMap, fromHistory);
      getDatabase().execute(saveFlexFields);
    }
  }


  @Override
  public JobPredictionOutput predictJobDuration(JobPredictionInput input) {
    TaskPredictionInput taskInput = new TaskPredictionInput(input.getJobId(), TaskType.MAP);
    completeInput(taskInput);
    Long mapDuration = predictMapTaskDuration(taskInput).getDuration() * taskInput.getJob().getTotalMapTasks();
    taskInput.setTaskType(TaskType.REDUCE);
    Long reduceDuration = predictReduceTaskDuration(taskInput).getDuration() * taskInput.getJob().getTotalReduceTasks();
    return new JobPredictionOutput(mapDuration + reduceDuration);
  }

  @Override
  public TaskPredictionOutput predictTaskDuration(TaskPredictionInput input) {
    completeInput(input);
    if (input.getTaskType().equals(TaskType.MAP))
      return predictMapTaskDuration(input);
    return predictReduceTaskDuration(input);
  }

  private TaskPredictionOutput handleNoMapHistory(JobProfile job) {
    if (orZero(job.getAvgMapDuration()) != 0)
      // if we do have at least the current average duration, return that, regardless of location
      return new TaskPredictionOutput(orZero(job.getAvgMapDuration()));
    logger.debug("No map history data for " + job.getId() + ". Using default");
    // return the default; there is nothing we can do
    return new TaskPredictionOutput(DEFAULT_TASK_DURATION);
  }

  private TaskPredictionOutput predictMapTaskDuration(TaskPredictionInput input) {
    JobProfile job = input.getJob();
    updateProfile(job, false);

    DetailedMapPredictionStats jobStats = new DetailedMapPredictionStats(1, 0);
    jobStats.addSource(job);

    Boolean local = null;
    if (input.getNodeAddress() != null)
      local = input.getTask().getSplitLocations().contains(input.getNodeAddress());

    Double rate = local == null ? jobStats.getAvgRate() : local ? jobStats.getAvgLocalRate() : jobStats.getAvgRemoteRate();

    if (rate == null) {
      // we don't know the rate of that type
      // get the appropriate average map processing rate from history
      DetailedMapPredictionStats mapStats = model.getRelevantMapStats(job.getMapperClass(), job.getUser());
      if (mapStats == null)
        return handleNoMapHistory(job);
      if (mapStats.getRelevance() > 1 && job.getAvgMapDuration() != null)
        // if history is not relevant and we have the current average duration, return it
        return new TaskPredictionOutput(job.getAvgMapDuration());
      rate = local == null ? mapStats.getAvgRate() : local ? mapStats.getAvgLocalRate() : mapStats.getAvgRemoteRate();
      if (rate == null)
        return handleNoMapHistory(job);
    }
    // multiply by how much input each task has
    Long splitSize = getSplitSize(input.getTask(), job);
    Double duration = splitSize / rate;
    logger.debug("Map duration for " + job.getId() + " should be " + splitSize + " / " + rate + "=" + duration);
    return new TaskPredictionOutput(duration.longValue());
  }

  private Double getMapTaskSelectivity(JobProfile job) {
    // we try to get the selectivity from the map history
    DetailedMapPredictionStats mapStats = model.getRelevantMapStats(job.getMapperClass(), job.getUser());
    if (mapStats == null || mapStats.getRelevance() > 1 || mapStats.getAvgSelectivity() == null) {
      // there is no history, or it is not relevant for selectivity, so get current selectivity
      String selectivityString = job.getFlexField(FLEX_KEY_PREFIX + MAP_SELECTIVITY);
      logger.debug("Using own selectivity for " + job.getId() + ": " + selectivityString);
      return selectivityString != null ? Double.valueOf(selectivityString) : null;
    }
    return mapStats.getAvgSelectivity();
  }

  private TaskPredictionOutput handleNoReduceHistory(JobProfile job, Double avgSelectivity) {
    if (avgSelectivity == 0 || job.getCompletedMaps() == 0 || job.getTotalInputBytes() == null) {
      // our selectivity or map rate data is unreliable
      // just return default duration
      logger.debug("No data to compute reduce for " + job.getName() + ". Using default");
      return new TaskPredictionOutput(DEFAULT_TASK_DURATION);
    }

    // calculate the current map rate and assume reduce rate is the same

    // we assume the reduce processing rate is the same as the map processing rate
    String durationString = job.getFlexField(FLEX_KEY_PREFIX + MAP_REMOTE);
    if (durationString == null)
      durationString = job.getFlexField(FLEX_KEY_PREFIX + MAP_LOCAL);
    Double mapRate = durationString == null ? orZero(job.getAvgMapDuration()) : Double.valueOf(durationString);
    // calculate how much input the task has
    // restrict to a minimum of 1 byte per task to avoid multiplication or division by zero
    Double inputPerTask = Math.max(orZero(job.getTotalInputBytes()) * avgSelectivity / job.getTotalReduceTasks(), 1);
    Double duration = inputPerTask / mapRate;
    logger.debug("Reduce duration computed based on map data for " + job.getId() + " as " + duration + "from (remote) mapRate=" + mapRate + " and selectivity=" + avgSelectivity);
    return new TaskPredictionOutput(duration.longValue());
  }

  private TaskPredictionOutput predictReduceTaskDuration(TaskPredictionInput input) {
    JobProfile job = input.getJob();
    updateProfile(job, false);

    Double avgSelectivity = getMapTaskSelectivity(job);

    DetailedReducePredictionStats jobStats = new DetailedReducePredictionStats(1, 0);
    jobStats.addSource(job);

    if (jobStats.isIncomplete()) {
      // if the typical shuffle rate is not calculated, we are clearly missing information
      // get averages from history
      DetailedReducePredictionStats reduceStats = model.getRelevantReduceStats(job.getReducerClass(), job.getUser());
      if (reduceStats != null) {
        if (avgSelectivity == null || reduceStats.getRelevance() > 1) {
          // our selectivity or reduce rate data is unreliable
          // just return average reduce duration of historical jobs
          logger.debug("Reduce duration calculated as simple average for " + job.getId() + " =  " + reduceStats.getAvgReduceDuration());
          return new TaskPredictionOutput(reduceStats.getAvgReduceDuration().longValue());
        }
        jobStats.completeFrom(reduceStats);
      }
    }

    if (jobStats.getAvgReduceDuration() == null)
      // we have no current or historical reduce information, not even average duration
      return handleNoReduceHistory(job, avgSelectivity);

    if (jobStats.getAvgMergeRate() != null && jobStats.getAvgReduceRate() != null) {
      // our selectivity and reduce rate data is reliable
      TaskPredictionOutput duration = predictReduceByPhases(job, avgSelectivity, jobStats);
      if (duration != null)
        return duration;
    }
    // we are missing information; guessing now won't work
    // just return average reduce duration of the current job or historical jobs

    logger.debug("Reduce duration calculated as simple average for " + job.getId() + " =  " + jobStats.getAvgReduceDuration());
    return new TaskPredictionOutput(jobStats.getAvgReduceDuration().longValue());
  }

  private TaskPredictionOutput predictReduceByPhases(JobProfile job, Double avgSelectivity, DetailedReducePredictionStats jobStats) {
    if (avgSelectivity == null)
      return null;
    // calculate how much input the task should have based on how much is left and how many reduces remain
    // restrict to a minimum of 1 byte per task to avoid multiplication or division by zero
    Double inputLeft = orZero(job.getTotalInputBytes()) * avgSelectivity - orZero(job.getReduceInputBytes());
    Double inputPerTask = Math.max(inputLeft / (job.getTotalReduceTasks() - job.getCompletedReduces()), 1);
    Long shuffleTime =
      predictShuffleTime(jobStats, ObjectUtils.equals(job.getCompletedMaps(), job.getTotalMapTasks()), inputPerTask);
    if (shuffleTime == null)
      return null;
    Double duration = shuffleTime + inputPerTask / jobStats.getAvgMergeRate() + inputPerTask / jobStats.getAvgReduceRate();
    logger.debug("Reduce duration for " + job.getId() + " should be " + shuffleTime + " + " +
      inputPerTask + " / " + jobStats.getAvgMergeRate() + " + " +
      inputPerTask + " / " + jobStats.getAvgReduceRate() + "=" + duration);
    return new TaskPredictionOutput(duration.longValue());
  }

  private Long predictShuffleTime(DetailedReducePredictionStats jobStats, boolean isFirstShuffle, Double inputPerTask) {
    if (isFirstShuffle) {
      if (jobStats.getAvgShuffleFirstTime() == null)
        return null;
      return jobStats.getAvgShuffleFirstTime().longValue();
    } else {
      if (jobStats.getAvgShuffleTypicalRate() == null)
        return null;
      return Double.valueOf(inputPerTask / jobStats.getAvgShuffleTypicalRate()).longValue();
    }
  }
}
