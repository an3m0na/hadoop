package org.apache.hadoop.tools.posum.simulation.predictor;

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
import org.apache.hadoop.tools.posum.common.util.conf.PosumConfiguration;
import org.apache.hadoop.tools.posum.simulation.predictor.basic.BasicPredictor;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.TASK;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.TASK_HISTORY;
import static org.apache.hadoop.tools.posum.common.util.GeneralUtils.orZero;

public abstract class JobBehaviorPredictor<M extends PredictionModel<P>, P extends PredictionProfile> {
  private static final Log logger = LogFactory.getLog(JobBehaviorPredictor.class);

  protected final Long defaultTaskDuration;
  protected Configuration conf;
  private Database db;
  protected int historyBuffer;
  protected M model;

  protected JobBehaviorPredictor(Configuration conf) {
    this.conf = conf;
    this.historyBuffer = conf.getInt(PosumConfiguration.PREDICTION_BUFFER, PosumConfiguration.PREDICTION_BUFFER_DEFAULT);
    this.defaultTaskDuration = conf.getLong(PosumConfiguration.AVERAGE_TASK_DURATION,
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
      P profile = buildPredictionProfile(job);
      savePredictionProfile(profile);
      model.updateModel(profile);
    }
  }

  protected abstract M initializeModel();

  protected abstract P buildPredictionProfile(JobProfile job);

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
    P predictionProfile = buildPredictionProfile(input.getJob());
    savePredictionProfile(predictionProfile);
    if (input.getTaskType().equals(TaskType.REDUCE))
      return predictReduceTaskBehavior(input, predictionProfile);
    return predictMapTaskBehavior(input, predictionProfile);
  }

  protected void savePredictionProfile(PredictionProfile predictionProfile) {
    JobProfile job = predictionProfile.getJob();
    Map<String, String> profileFields = predictionProfile.serialize();
    if (profileFields != null && !profileFields.isEmpty()) {
      SaveJobFlexFieldsCall saveFlexFields = SaveJobFlexFieldsCall.newInstance(job.getId(), profileFields, job.getFinishTime() != null);
      getDatabase().execute(saveFlexFields);
      job.addAllFlexFields(profileFields);
    }
  }

  protected abstract TaskPredictionOutput predictMapTaskBehavior(TaskPredictionInput input, P predictionProfile);

  protected abstract TaskPredictionOutput predictReduceTaskBehavior(TaskPredictionInput input, P predictionProfile);

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
    return input;
  }

  private <E extends PredictionStatEntry<E>, T extends PredictionStats<E>> E getStatEntry(Enum key, T historicalStats, T jobStats, boolean forceRelevance) {
    E historicalEntry = historicalStats == null ? null : historicalStats.getEntry(key);
    E currentEntry = jobStats == null ? null : jobStats.getEntry(key);

    // we try to get the average from the map history
    if (historicalEntry == null) {
      // we have no historical information about this job current average if it exists
      return currentEntry;
    }
    // we have historical information
    if (historicalStats.getRelevance() <= 1)
      // historical info is relevant, so prefer it
      return historicalEntry;
    if (forceRelevance)
      // history is not relevant, so return current average if it exists
      return currentEntry;
    return currentEntry == null ? historicalEntry : currentEntry;
  }

  protected <E extends PredictionStatEntry<E>, T extends PredictionStats<E>> E getRelevantStatEntry(Enum key, T historicalStats, T jobStats) {
    return getStatEntry(key, historicalStats, jobStats, true);
  }

  protected <E extends PredictionStatEntry<E>, T extends PredictionStats<E>> E getAnyStatEntry(Enum key, T historicalStats, T jobStats) {
    return getStatEntry(key, historicalStats, jobStats, false);
  }

  protected TaskPredictionOutput handleNoMapInfo(JobProfile job) {
    logger.trace("Insufficient map data for " + job.getId() + ". Using default");
    // return the default; there is nothing we can do
    return new TaskPredictionOutput(defaultTaskDuration);
  }

  protected TaskPredictionOutput handleNoReduceInfo(JobProfile job) {
    logger.trace("Insufficient reduce data for " + job.getName() + ". Using default");
    // return the default; there is nothing we can do
    return new TaskPredictionOutput(defaultTaskDuration);
  }

  protected List<TaskProfile> getJobTasks(String jobId, boolean fromHistory) {
    FindByQueryCall getTasks = FindByQueryCall.newInstance(fromHistory ? TASK_HISTORY : TASK,
      QueryUtils.is("jobId", jobId));
    return getDatabase().execute(getTasks).getEntities();
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

  public void clearHistory() {
    model = null;
  }
}
