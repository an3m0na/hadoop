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
import org.apache.hadoop.tools.posum.simulation.predictor.basic.BasicPredictor;

import java.util.List;

public abstract class JobBehaviorPredictor {

  protected final Long DEFAULT_JOB_DURATION;
  protected final Long DEFAULT_TASK_DURATION;
  protected Configuration conf;
  private Database db;
  protected int historyBuffer;

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

  public void switchDatabase(Database db){
    this.db = db;
  }

    /* WARNING! Prediction methods may throw exceptions if data model changes occur during computation (e.g. task finishes) */

  public abstract JobPredictionOutput predictJobDuration(JobPredictionInput input);

  public abstract TaskPredictionOutput predictTaskDuration(TaskPredictionInput input);

  protected TaskPredictionInput completeInput(TaskPredictionInput input) {
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
    List<JobProfile> comparable = getDatabase().execute(getComparableJobs).getEntities();
    if (comparable.size() < 1) {
      // get past jobs at least by the same user
      getComparableJobs.setQuery(QueryUtils.is("user", user));
      comparable = getDatabase().execute(getComparableJobs).getEntities();
    }
    return comparable;
  }
}
