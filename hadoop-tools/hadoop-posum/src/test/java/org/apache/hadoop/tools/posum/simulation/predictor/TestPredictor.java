package org.apache.hadoop.tools.posum.simulation.predictor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.tools.posum.client.data.Database;
import org.apache.hadoop.tools.posum.common.records.call.StoreAllCall;
import org.apache.hadoop.tools.posum.common.records.call.TransactionCall;
import org.apache.hadoop.tools.posum.common.records.dataentity.DatabaseReference;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.TaskProfile;
import org.apache.hadoop.tools.posum.common.util.conf.PosumConfiguration;
import org.apache.hadoop.tools.posum.data.mock.data.HistorySnapshotStore;
import org.apache.hadoop.tools.posum.test.Utils;
import org.apache.hadoop.yarn.util.Records;
import org.junit.Before;

import java.util.Arrays;

import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.JOB;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.TASK;

public abstract class TestPredictor<T extends JobBehaviorPredictor> {
  protected static final int BUFFER_LIMIT = 2;
  protected static final long DEFAULT_TASK_DURATION = 1500;

  protected T predictor;
  protected Database db;

  protected JobProfile someJob = Records.newRecord(JobProfile.class);
  protected JobProfile anotherJob = Records.newRecord(JobProfile.class);
  protected TaskProfile someJobMapTask = Records.newRecord(TaskProfile.class);
  protected TaskProfile someJobReduceTask = Records.newRecord(TaskProfile.class);
  protected TaskProfile anotherJobMapTask = Records.newRecord(TaskProfile.class);
  protected TaskProfile anotherJobReduceTask = Records.newRecord(TaskProfile.class);

  public TestPredictor(Class<T> predictorClass) {
    Configuration conf = PosumConfiguration.newInstance();
    conf.set(PosumConfiguration.PREDICTION_BUFFER, Integer.toString(BUFFER_LIMIT));
    conf.set(PosumConfiguration.AVERAGE_TASK_DURATION, Long.toString(DEFAULT_TASK_DURATION));

    predictor = JobBehaviorPredictor.newInstance(conf, predictorClass);
  }

  @Before
  public void setUp() throws Exception {
    HistorySnapshotStore historySnapshotStore = Utils.mockDefaultWorkload();
    historySnapshotStore.setSnapshotTime(historySnapshotStore.getTraceFinishTime() + 1);

    db = Database.from(historySnapshotStore, DatabaseReference.getMain());

    someJob.setId("someJob");
    someJob.setUser("test_user_1");

    anotherJob.setId("anotherJob");
    anotherJob.setUser("anotherJobUser");

    someJobMapTask.setId("someJobMapTask");
    someJobMapTask.setJobId(someJob.getId());
    someJobMapTask.setType(TaskType.MAP);

    someJobReduceTask.setId("someJobReduceTask");
    someJobReduceTask.setJobId(someJob.getId());
    someJobReduceTask.setType(TaskType.REDUCE);

    anotherJobMapTask.setId("anotherJobMapTask");
    anotherJobMapTask.setJobId(anotherJob.getId());
    anotherJobMapTask.setType(TaskType.MAP);

    anotherJobReduceTask.setId("anotherJobReduceTask");
    anotherJobReduceTask.setJobId(anotherJob.getId());
    anotherJobReduceTask.setType(TaskType.REDUCE);

    TransactionCall transaction = TransactionCall.newInstance()
      .addCall(StoreAllCall.newInstance(JOB,
        Arrays.asList(someJob, anotherJob)))
      .addCall(StoreAllCall.newInstance(TASK,
        Arrays.asList(
          someJobMapTask,
          someJobReduceTask,
          anotherJobMapTask,
          anotherJobReduceTask
        )));
    db.execute(transaction);

    predictor.train(db);
  }
}
