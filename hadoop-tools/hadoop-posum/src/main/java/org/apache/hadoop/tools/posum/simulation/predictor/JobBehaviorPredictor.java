package org.apache.hadoop.tools.posum.simulation.predictor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.tools.posum.client.data.Database;
import org.apache.hadoop.tools.posum.common.records.call.FindByIdCall;
import org.apache.hadoop.tools.posum.common.records.call.IdsByQueryCall;
import org.apache.hadoop.tools.posum.common.records.call.SaveJobFlexFieldsCall;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.TaskProfile;
import org.apache.hadoop.tools.posum.common.util.PosumConfiguration;
import org.apache.hadoop.tools.posum.common.util.PosumException;
import org.apache.hadoop.tools.posum.simulation.predictor.basic.BasicPredictor;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.tools.posum.common.util.Utils.orZero;

public abstract class JobBehaviorPredictor<M extends PredictionModel> {

  protected final Long DEFAULT_TASK_DURATION;
  protected Configuration conf;
  private Database db;
  protected int historyBuffer;
  protected M model;

  protected JobBehaviorPredictor(Configuration conf) {
    this.conf = conf;
    this.historyBuffer = conf.getInt(PosumConfiguration.PREDICTION_BUFFER, PosumConfiguration.PREDICTION_BUFFER_DEFAULT);
    this.DEFAULT_TASK_DURATION = conf.getLong(PosumConfiguration.AVERAGE_TASK_DURATION,
      PosumConfiguration.AVERAGE_TASK_DURATION_DEFAULT);
  }

  public static JobBehaviorPredictor newInstance(Configuration conf) {
    return newInstance(conf, conf.getClass(
      PosumConfiguration.PREDICTOR_CLASS,
      BasicPredictor.class,
      JobBehaviorPredictor.class
    ));
  }

  public static <T extends JobBehaviorPredictor> T newInstance(Configuration conf,
                                                 Class<T> predictorClass) {
    try {
      return predictorClass.getConstructor(Configuration.class).newInstance(conf);
    } catch (Exception e) {
      throw new PosumException("Could not instantiate predictor type " + predictorClass.getName(), e);
    }
  }

  public void train(Database db) {
    if (model == null)
      model = initializeModel();
    this.db = db;

    // check for new finished jobs and train on them
    IdsByQueryCall getFinishedJobIds = IdsByQueryCall.newInstance(DataEntityCollection.JOB_HISTORY, null, "finishTime", false);
    List<String> historyJobIds = new LinkedList<>(db.execute(getFinishedJobIds).getEntries());
    historyJobIds.removeAll(model.getSourceJobs());
    for (String jobId : historyJobIds) {
      FindByIdCall getJob = FindByIdCall.newInstance(DataEntityCollection.JOB_HISTORY, jobId);
      JobProfile job = getDatabase().execute(getJob).getEntity();
      updatePredictionProfile(job, true);
      model.updateModel(job);
    }
  }

  protected abstract M initializeModel();

  protected void updatePredictionProfile(JobProfile job, boolean fromHistory) {
    Map<String, String> fieldMap = getPredictionProfileUpdates(job, fromHistory);

    if (fieldMap != null && !fieldMap.isEmpty()) {
      job.getFlexFields().putAll(fieldMap);
      SaveJobFlexFieldsCall saveFlexFields = SaveJobFlexFieldsCall.newInstance(job.getId(), fieldMap, fromHistory);
      getDatabase().execute(saveFlexFields);
    }
  }

  protected abstract Map<String, String> getPredictionProfileUpdates(JobProfile job, boolean fromHistory);

  public void switchDatabase(Database db) {
    this.db = db;
  }

  /* WARNING! Prediction methods may throw exceptions if data model changes occur during computation (e.g. task finishes) */

  public JobPredictionOutput predictJobBehavior(JobPredictionInput input) {
    FindByIdCall getJob = FindByIdCall.newInstance(DataEntityCollection.JOB, input.getJobId());
    JobProfile job = getDatabase().execute(getJob).getEntity();
    Long mapDuration = predictTaskBehavior(new TaskPredictionInput(job, TaskType.MAP)).getDuration();
    Long reduceDuration = predictTaskBehavior(new TaskPredictionInput(job, TaskType.REDUCE)).getDuration();
    return new JobPredictionOutput(mapDuration * orZero(job.getTotalMapTasks()) +
      reduceDuration * orZero(job.getTotalReduceTasks()));
  }

  public TaskPredictionOutput predictTaskBehavior(TaskPredictionInput input) {
    completeInput(input);
    if (input.getTaskType().equals(TaskType.REDUCE))
      return predictReduceTaskBehavior(input);
    return predictMapTaskBehavior(input);
  }

  protected abstract TaskPredictionOutput predictMapTaskBehavior(TaskPredictionInput input);

  protected abstract TaskPredictionOutput predictReduceTaskBehavior(TaskPredictionInput input);

  private TaskPredictionInput completeInput(TaskPredictionInput input) {
    if (input.getTaskId() != null) {
      if (input.getTask() == null) {
        input.setTask(getTaskById(input.getTaskId()));
        input.setTaskType(input.getTask().getType());
      }
    }
    if (input.getTask() != null) {
      if (input.getJob() == null)
        input.setJob(getJobById(input.getTask().getJobId()));
    } else {
      if (input.getJob() == null) {
        // no specific task => must have at least jobId and type
        if (input.getJobId() == null || input.getTaskType() == null)
          throw new PosumException("Too little information for prediction! Input: " + input);
        input.setJob(getJobById(input.getJobId()));
      }
    }
    updatePredictionProfile(input.getJob(), false);
    return input;
  }

  private JobProfile getJobById(String jobId) {
    FindByIdCall getJobById = FindByIdCall.newInstance(DataEntityCollection.JOB, jobId);
    JobProfile job = getDatabase().execute(getJobById).getEntity();
    if (job == null)
      throw new PosumException("Job not found or finished for id " + jobId);
    return job;
  }

  private TaskProfile getTaskById(String taskId) {
    FindByIdCall getTaskById = FindByIdCall.newInstance(DataEntityCollection.TASK, taskId);
    TaskProfile task = getDatabase().execute(getTaskById).getEntity();
    if (task == null)
      throw new PosumException("Task not found or finished for id " + taskId);
    return task;
  }

  protected Database getDatabase() {
    if (db == null)
      throw new PosumException("Database not initialized in Predictor");
    return db;
  }

  public M getModel() {
    return model;
  }

}
