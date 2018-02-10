package org.apache.hadoop.tools.posum.simulation.predictor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.tools.posum.client.data.Database;
import org.apache.hadoop.tools.posum.common.records.call.FindByIdCall;
import org.apache.hadoop.tools.posum.common.records.call.StoreAllCall;
import org.apache.hadoop.tools.posum.common.records.call.StoreCall;
import org.apache.hadoop.tools.posum.common.records.call.TransactionCall;
import org.apache.hadoop.tools.posum.common.records.call.UpdateOrStoreCall;
import org.apache.hadoop.tools.posum.common.records.dataentity.DatabaseReference;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.TaskProfile;
import org.apache.hadoop.tools.posum.common.util.conf.PosumConfiguration;
import org.apache.hadoop.tools.posum.data.mock.data.HistorySnapshotStore;
import org.apache.hadoop.tools.posum.test.Utils;
import org.apache.hadoop.yarn.util.Records;
import org.junit.Before;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hadoop.mapreduce.v2.api.records.TaskType.MAP;
import static org.apache.hadoop.mapreduce.v2.api.records.TaskType.REDUCE;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.JOB;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.TASK;

public abstract class TestPredictor<T extends JobBehaviorPredictor> {
  protected static final int BUFFER_LIMIT = 2;
  protected static final long DEFAULT_TASK_DURATION = 1500;
  protected static volatile AtomicInteger JOB_ID = new AtomicInteger(0);
  protected static volatile AtomicInteger EXTRA_TASK_ID = new AtomicInteger(0);
  protected static volatile String JOB_ID_PREFIX = "predictor_job_";

  protected T predictor;
  protected Database db;

  public TestPredictor(Class<T> predictorClass) {
    Configuration conf = PosumConfiguration.newInstance();
    conf.set(PosumConfiguration.PREDICTION_BUFFER, Integer.toString(BUFFER_LIMIT));
    conf.set(PosumConfiguration.AVERAGE_TASK_DURATION, Long.toString(DEFAULT_TASK_DURATION));

    predictor = JobBehaviorPredictor.newInstance(conf, predictorClass);
  }

  @Before
  public void setUp() {
    HistorySnapshotStore historySnapshotStore = Utils.mockDefaultWorkload();
    historySnapshotStore.setSnapshotTime(historySnapshotStore.getTraceFinishTime() + 1);

    db = Database.from(historySnapshotStore, DatabaseReference.getMain());
    predictor.train(db);
  }

  protected JobProfile createJob(String user) {
    JobProfile job = Records.newRecord(JobProfile.class);
    job.setId(JOB_ID_PREFIX + JOB_ID.incrementAndGet());
    job.setUser(user);
    job.setTotalMapTasks(1);
    job.setTotalReduceTasks(1);

    TaskProfile mapTask = Records.newRecord(TaskProfile.class);
    mapTask.setId(job.getId() + "_" + MAP.name());
    mapTask.setJobId(job.getId());
    mapTask.setType(MAP);

    TaskProfile reduceTask = Records.newRecord(TaskProfile.class);
    reduceTask.setId(job.getId() + "_" + REDUCE.name());
    reduceTask.setJobId(job.getId());
    reduceTask.setType(TaskType.REDUCE);

    TransactionCall transaction = TransactionCall.newInstance()
      .addCall(StoreCall.newInstance(JOB, job))
      .addCall(StoreAllCall.newInstance(TASK, Arrays.asList(mapTask, reduceTask)));
    db.execute(transaction);
    return job;
  }

  protected TaskProfile addTask(String jobId, TaskType type) {
    TaskProfile task = Records.newRecord(TaskProfile.class);
    task.setId(jobId + "_" + type.name() + "_" + EXTRA_TASK_ID.incrementAndGet());
    task.setJobId(jobId);
    task.setType(type);
    db.execute(StoreCall.newInstance(TASK, task));
    return task;
  }

  protected TaskProfile getTaskForJob(String jobId, TaskType type) {
    return db.execute(FindByIdCall.newInstance(TASK, jobId + "_" + type.name())).getEntity();
  }

  protected void save(TaskProfile task) {
    db.execute(UpdateOrStoreCall.newInstance(TASK, task));
  }

  protected JobProfile createJobOnKnownUser() {
    return createJob("test_user_1");
  }

  protected JobProfile createJobOnUnknownUser() {
    return createJob("anotherJobUser");
  }
}
