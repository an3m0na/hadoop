package org.apache.hadoop.tools.posum.simulation.predictor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.tools.posum.common.records.call.FindByIdCall;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;

import java.util.List;

public class StandardPredictor extends JobBehaviorPredictor {

  private static final Log logger = LogFactory.getLog(StandardPredictor.class);

  public StandardPredictor(Configuration conf) {
    super(conf);
  }

  @Override
  public JobPredictionOutput predictJobDuration(JobPredictionInput input) {
    FindByIdCall getJob = FindByIdCall.newInstance(DataEntityCollection.JOB, input.getJobId());
    JobProfile job = getDatabase().executeDatabaseCall(getJob).getEntity();
    return new JobPredictionOutput(predictMapTaskDuration(job).getDuration() * job.getTotalMapTasks() +
      predictReduceTaskDuration(job).getDuration() * job.getTotalReduceTasks());
  }

  private TaskPredictionOutput predictMapTaskDuration(JobProfile job) {
    if (job.getAvgMapDuration() != 0)
      return new TaskPredictionOutput(job.getAvgMapDuration());
    // we have no information about this job; predict from history
    List<JobProfile> comparable = getComparableProfilesByType(job, TaskType.MAP);
    if (comparable.size() < 1) {
      logger.debug("No map history data for " + job.getId() + ". Using default");
      return new TaskPredictionOutput(DEFAULT_TASK_DURATION);
    }
    Double avgMapRate = 0.0;
    for (JobProfile profile : comparable) {
      //restrict to a minimum of 1 byte per task to avoid multiplication or division by zero
      Long inputPerMap = Math.max(job.getTotalInputBytes() / job.getTotalMapTasks(), 1);
      avgMapRate += 1.0 * inputPerMap / profile.getAvgMapDuration();
    }
    avgMapRate /= comparable.size();
    // restrict to a minimum of 1 byte per task to avoid multiplication or division by zero
    Long inputPerMap = Math.max(job.getTotalInputBytes() / job.getTotalMapTasks(), 1);
    Double duration = 1.0 * inputPerMap / avgMapRate;
    logger.debug("Map duration for " + job.getId() + " should be " + inputPerMap + " / " + avgMapRate + " = " + duration);
    return new TaskPredictionOutput(duration.longValue());
  }

  private Double calculateMapTaskSelectivity(JobProfile job) {
    // we try to compute selectivity from the map history
    List<JobProfile> comparable = getComparableProfilesByType(job, TaskType.MAP);
    if (comparable.size() < 1 || !comparable.get(0).getMapperClass().equals(job.getMapperClass())) {
      // there is no history, or it is not relevant for selectivity
      if (job.getCompletedMaps() > 0) {
        // we know the current selectivity
        // restrict to a minimum of 1 byte per task to avoid multiplication or division by zero
        Long inputPerMap = Math.max(job.getTotalInputBytes() / job.getTotalMapTasks(), 1);
        Long parsedInputBytes = job.getCompletedMaps() * inputPerMap;
        Double ownSelectivity = 1.0 * job.getMapOutputBytes() / parsedInputBytes;
        logger.debug("Using own selectivity for " + job.getId() + ": " + ownSelectivity);
        return ownSelectivity;
      }
      // we don't know anything about selectivity
      return 0.0;
    }
    Double avgSelectivity = 0.0;
    for (JobProfile profile : comparable) {
      // restrict to a minimum of 1 byte per task to avoid multiplication or division by zero
      logger.debug("Comparing " + job.getId() + " with other for selectivity: " + profile.getId());
      avgSelectivity += 1.0 * profile.getMapOutputBytes() / Math.max(profile.getTotalInputBytes(), profile.getTotalMapTasks());
    }
    return avgSelectivity / comparable.size();
  }

  private TaskPredictionOutput handleNoReduceHistory(JobProfile job, Double avgSelectivity) {
    if (avgSelectivity == 0 || job.getCompletedMaps() == 0) {
      // our selectivity or map rate data is unreliable
      // just return default duration
      logger.debug("No data to compute reduce for " + job.getName() + ". Using default");
      return new TaskPredictionOutput(DEFAULT_TASK_DURATION);
    }

    // calculate the current map rate and assume reduce rate is the same

    // restrict to a minimum of 1 byte per task to avoid multiplication or division by zero
    Long parsedInputBytes = job.getCompletedMaps() * Math.max(job.getTotalInputBytes() / job.getTotalMapTasks(), 1);
    Double mapRate = 1.0 * parsedInputBytes / job.getAvgMapDuration();
    // we assume the reduce processing rate is the same as the map processing rate
    Double inputPerTask = Math.max(job.getTotalInputBytes(), 1) * avgSelectivity / job.getTotalReduceTasks();
    Double duration = inputPerTask / mapRate;
    logger.debug("Reduce duration computed based on map data for " + job.getId() + " as " + duration + "from mapRate=" + mapRate + " and selectivity=" + avgSelectivity);
    return new TaskPredictionOutput(duration.longValue());
  }

  private TaskPredictionOutput predictReduceTaskDuration(JobProfile job) {
    if (job.getAvgReduceDuration() != 0)
      return new TaskPredictionOutput(job.getAvgReduceDuration());

    // calculate average duration based on map selectivity and historical processing rates

    Double avgSelectivity = calculateMapTaskSelectivity(job);

    List<JobProfile> comparable = getComparableProfilesByType(job, TaskType.REDUCE);
    if (comparable.size() < 1) {
      // non-existent or irrelevant historical data
      return handleNoReduceHistory(job, avgSelectivity);
    }
    boolean relevantHistory = comparable.get(0).getReducerClass().equals(job.getReducerClass());

    // we have reduce history; calculate reduce rate
    Double avgReduceRate = 0.0, avgReduceDuration = 0.0;
    Integer comparableNo = 0;
    for (JobProfile profile : comparable) {
      if (profile.getTotalReduceTasks() < 1)
        continue;
      logger.debug("Comparing reduce of " + job.getId() + " with " + profile.getId());
      comparableNo++;
      avgReduceDuration += profile.getAvgReduceDuration();
      if (avgSelectivity != 0 && relevantHistory) {
        // calculate reduce rate; restrict to a minimum of 1 byte per task to avoid multiplication or division by zero
        Double inputPerTask = 1.0 * Math.max(profile.getReduceInputBytes() / profile.getTotalReduceTasks(), 1);
        avgReduceRate += inputPerTask / profile.getAvgReduceDuration();
      }
    }
    if (comparableNo < 1)
      // non-existent or irrelevant historical data
      return handleNoReduceHistory(job, avgSelectivity);
    avgReduceDuration /= comparableNo;
    if (avgSelectivity == 0 || !relevantHistory) {
      // our selectivity or reduce rate data is unreliable
      // just return average reduce duration of historical jobs
      logger.debug("Reduce duration calculated as simple average for " + job.getId() + " =  " + avgReduceDuration);
      return new TaskPredictionOutput(avgReduceDuration.longValue());
    }
    avgReduceRate /= comparableNo;
    // restrict to a minimum of 1 byte per task to avoid multiplication or division by zero
    Double inputPerTask = Math.max(job.getTotalInputBytes() * avgSelectivity / job.getTotalReduceTasks(), 1);
    Double duration = inputPerTask / avgReduceRate;
    logger.debug("Reduce duration for " + job.getId() + " should be " + inputPerTask + " / " + avgReduceRate + "=" + duration);
    return new TaskPredictionOutput(duration.longValue());
  }

  @Override
  public TaskPredictionOutput predictTaskDuration(TaskPredictionInput input) {
    completeInput(input);
    if (input.getTaskType().equals(TaskType.REDUCE))
      return predictReduceTaskDuration(input.getJob());
    return predictMapTaskDuration(input.getJob());
  }
}
