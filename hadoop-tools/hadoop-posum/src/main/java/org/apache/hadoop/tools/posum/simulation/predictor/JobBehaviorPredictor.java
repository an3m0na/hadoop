package org.apache.hadoop.tools.posum.simulation.predictor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.tools.posum.client.data.Database;
import org.apache.hadoop.tools.posum.common.records.call.FindByIdCall;
import org.apache.hadoop.tools.posum.common.records.call.FindByQueryCall;
import org.apache.hadoop.tools.posum.common.records.call.query.DatabaseQuery;
import org.apache.hadoop.tools.posum.common.records.call.query.QueryUtils;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.TaskProfile;
import org.apache.hadoop.tools.posum.common.util.PosumConfiguration;
import org.apache.hadoop.tools.posum.common.util.PosumException;

import java.util.List;

public abstract class JobBehaviorPredictor {

  protected final Long DEFAULT_JOB_DURATION;
  protected final Long DEFAULT_TASK_DURATION;
  protected Configuration conf;
  private Database db;
  private int historyBuffer;

  protected JobBehaviorPredictor(Configuration conf) {
    this.conf = conf;
    this.historyBuffer = conf.getInt(PosumConfiguration.PREDICTION_BUFFER, PosumConfiguration.PREDICTION_BUFFER_DEFAULT);
    this.DEFAULT_JOB_DURATION = conf.getLong(PosumConfiguration.AVERAGE_JOB_DURATION,
      PosumConfiguration.AVERAGE_JOB_DURATION_DEFAULT);
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

  public static JobBehaviorPredictor newInstance(Configuration conf,
                                                 Class<? extends JobBehaviorPredictor> predictorClass) {
    try {
      return predictorClass.getConstructor(Configuration.class).newInstance(conf);
    } catch (Exception e) {
      throw new PosumException("Could not instantiate predictor type " + predictorClass.getName(), e);
    }
  }

  public void initialize(Database db) {
    this.db = db;
  }

    /* WARNING! Prediction methods may throw exceptions if data model changes occur during computation (e.g. task finishes) */

  public abstract JobPredictionOutput predictJobDuration(JobPredictionInput input);

  public abstract TaskPredictionOutput predictTaskDuration(TaskPredictionInput input);

  protected TaskPredictionInput completeInput(TaskPredictionInput input) {
    if (input.getJob() != null && input.getTaskType() != null)
      return input;

    FindByIdCall getJobById = FindByIdCall.newInstance(DataEntityCollection.JOB, null);
    if (input.getJobId() != null && input.getTaskType() != null) {
      getJobById.setId(input.getJobId());
      JobProfile job = getDatabase().executeDatabaseCall(getJobById).getEntity();
      if (job == null)
        throw new PosumException("Job not found or finished for id " + input.getJobId());
      input.setJob(job);
      return input;
    }
    if (input.getJobId() == null)
      throw new PosumException("Too little information for prediction! Input: " + input);
    FindByIdCall getById = FindByIdCall.newInstance(DataEntityCollection.TASK, input.getTaskId());
    TaskProfile task = getDatabase().executeDatabaseCall(getById).getEntity();
    if (task == null)
      throw new PosumException("Task not found or finished for id " + input.getTaskId());

    input.setTaskType(task.getType());
    getJobById.setId(input.getJobId());
    JobProfile job = getDatabase().executeDatabaseCall(getJobById).getEntity();
    input.setJob(job);

    return input;
  }

  Database getDatabase() {
    if (db == null)
      throw new PosumException("Database not initialized in Predictor");
    return db;
  }

  protected List<JobProfile> getComparableProfilesByName(JobProfile job) {
    return getComparableProfilesForUser(job.getUser(), QueryUtils.is("name", job.getName()));
  }

  protected List<JobProfile> getComparableProfilesByType(JobProfile job, TaskType type) {
    return getComparableProfilesForUser(job.getUser(),
      QueryUtils.is(type.equals(TaskType.MAP) ? "mapperClass" : "reducerClass",
        type.equals(TaskType.MAP) ? job.getMapperClass() : job.getReducerClass()));
  }

  private List<JobProfile> getComparableProfilesForUser(String user, DatabaseQuery detailedQuery) {
    FindByQueryCall getComparableJobs = FindByQueryCall.newInstance(
      DataEntityCollection.JOB_HISTORY,
      detailedQuery,
      -historyBuffer,
      historyBuffer
    );
    List<JobProfile> comparable = getDatabase().executeDatabaseCall(getComparableJobs).getEntities();
    if (comparable.size() < 1) {
      // get past jobs at least by the same user
      getComparableJobs.setQuery(QueryUtils.is("user", user));
      comparable = getDatabase().executeDatabaseCall(getComparableJobs).getEntities();
    }
    return comparable;
  }
}
