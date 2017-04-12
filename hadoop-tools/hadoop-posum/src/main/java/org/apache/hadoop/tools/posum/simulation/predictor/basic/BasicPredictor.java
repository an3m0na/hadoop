package org.apache.hadoop.tools.posum.simulation.predictor.basic;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.tools.posum.common.records.call.FindByIdCall;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.simulation.predictor.JobBehaviorPredictor;
import org.apache.hadoop.tools.posum.simulation.predictor.JobPredictionInput;
import org.apache.hadoop.tools.posum.simulation.predictor.JobPredictionOutput;
import org.apache.hadoop.tools.posum.simulation.predictor.TaskPredictionInput;
import org.apache.hadoop.tools.posum.simulation.predictor.TaskPredictionOutput;

import java.util.List;

import static org.apache.hadoop.tools.posum.common.util.Utils.getDuration;
import static org.apache.hadoop.tools.posum.common.util.Utils.orZero;

public class BasicPredictor extends JobBehaviorPredictor {

  private static final Log logger = LogFactory.getLog(BasicPredictor.class);

  public BasicPredictor(Configuration conf) {
    super(conf);
  }

  @Override
  public JobPredictionOutput predictJobDuration(JobPredictionInput input) {
    FindByIdCall getJob = FindByIdCall.newInstance(DataEntityCollection.JOB, input.getJobId());
    JobProfile job = getDatabase().execute(getJob).getEntity();
    List<JobProfile> comparable = getComparableProfilesByName(job);
    if (comparable.size() < 1)
      return new JobPredictionOutput(DEFAULT_JOB_DURATION);
    Long duration = 0L;
    for (JobProfile pastJob : comparable)
      duration += getDuration(pastJob);
    duration /= comparable.size();
    return new JobPredictionOutput(duration);
  }

  @Override
  public TaskPredictionOutput predictTaskDuration(TaskPredictionInput input) {
    completeInput(input);
    long currentAverage = orZero(TaskType.MAP.equals(input.getTaskType()) ? input.getJob().getAvgMapDuration() :
      input.getJob().getAvgReduceDuration());
    if (currentAverage > 0)
      return new TaskPredictionOutput(currentAverage);

    List<JobProfile> comparable = getComparableProfilesByName(input.getJob());
    Long duration = 0L;
    int comparableNo = 0;
    for (JobProfile pastJob : comparable) {
      long newAverage = orZero(TaskType.MAP.equals(input.getTaskType()) ? pastJob.getAvgMapDuration() :
        pastJob.getAvgReduceDuration());
      if (currentAverage > 0) {
        duration += newAverage;
        comparableNo++;
      }
    }
    if (duration > 0 && comparableNo > 0)
      return new TaskPredictionOutput(duration / comparableNo);
    return new TaskPredictionOutput(DEFAULT_TASK_DURATION);
  }
}
