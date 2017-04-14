package org.apache.hadoop.tools.posum.simulation.predictor.basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.tools.posum.client.data.Database;
import org.apache.hadoop.tools.posum.common.records.call.IdsByQueryCall;
import org.apache.hadoop.tools.posum.common.records.call.StoreAllCall;
import org.apache.hadoop.tools.posum.common.records.call.TransactionCall;
import org.apache.hadoop.tools.posum.common.records.dataentity.DatabaseReference;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.TaskProfile;
import org.apache.hadoop.tools.posum.common.util.PosumConfiguration;
import org.apache.hadoop.tools.posum.data.mock.data.HistorySnapshotStore;
import org.apache.hadoop.tools.posum.simulation.predictor.TaskPredictionInput;
import org.apache.hadoop.tools.posum.simulation.predictor.TaskPredictionOutput;
import org.apache.hadoop.tools.posum.simulation.predictor.TestPredictor;
import org.apache.hadoop.tools.posum.test.Utils;
import org.apache.hadoop.yarn.util.Records;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.JOB;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.JOB_HISTORY;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.TASK;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

public class TestBasicPredictor extends TestPredictor<BasicPredictor> {
  private static final int BUFFER_LIMIT = 2;
  private static final long DEFAULT_TASK_DURATION=1500;
  private Database db;
  private BasicPredictor subject;

  private JobProfile someJob = Records.newRecord(JobProfile.class);
  private JobProfile anotherJob = Records.newRecord(JobProfile.class);
  private TaskProfile someJobMapTask = Records.newRecord(TaskProfile.class);
  private TaskProfile someJobReduceTask = Records.newRecord(TaskProfile.class);
  private TaskProfile anotherJobMapTask = Records.newRecord(TaskProfile.class);
  private TaskProfile anotherJobReduceTask = Records.newRecord(TaskProfile.class);


  public TestBasicPredictor() {
    super(BasicPredictor.class);
  }

  @Override
  public void setUp() throws Exception {
//    super.setUp();
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

    Configuration conf = PosumConfiguration.newInstance();
    conf.getInt(PosumConfiguration.PREDICTION_BUFFER, PosumConfiguration.PREDICTION_BUFFER_DEFAULT);
    conf.set(PosumConfiguration.PREDICTION_BUFFER, Integer.toString(BUFFER_LIMIT));
    conf.set(PosumConfiguration.AVERAGE_TASK_DURATION, Long.toString(DEFAULT_TASK_DURATION));
    subject = new BasicPredictor(conf);
    subject.train(db);
  }

  @Test
  public void testModelMakeup() throws Exception {
    Set<String> sourceJobs = subject.getModel().getSourceJobs();
    List<String> historicalJobs = db.execute(IdsByQueryCall.newInstance(JOB_HISTORY, null)).getEntries();
    assertThat(sourceJobs, containsInAnyOrder(historicalJobs.toArray()));

    BasicPredictionStats someJobStats = subject.getModel().getRelevantStats(someJob);
    assertThat(someJobStats.getAvgMapDuration(), is(2801.0));
    assertThat(someJobStats.getAvgReduceDuration(), is(66321.0));

    BasicPredictionStats anotherJobStats = subject.getModel().getRelevantStats(anotherJob);
    assertThat(anotherJobStats, nullValue());
  }

  @Test
  public void testMapPrediction() throws Exception {
    TaskPredictionOutput prediction = subject.predictTaskBehavior(new TaskPredictionInput(someJob.getId(), TaskType.MAP));
    assertThat(prediction.getDuration(), is(2801L));

    someJob.setAvgMapDuration(1000L);
    prediction = subject.predictTaskBehavior(new TaskPredictionInput(someJob.getId(), TaskType.MAP));
    assertThat(prediction.getDuration(), is(1000L));

    prediction = subject.predictTaskBehavior(new TaskPredictionInput(anotherJob.getId(), TaskType.MAP));
    assertThat(prediction.getDuration(), is(DEFAULT_TASK_DURATION));

    anotherJob.setAvgMapDuration(2000L);
    prediction = subject.predictTaskBehavior(new TaskPredictionInput(anotherJob.getId(), TaskType.MAP));
    assertThat(prediction.getDuration(), is(2000L));
  }

  @Test
  public void testReducePrediction() throws Exception {
    TaskPredictionOutput prediction = subject.predictTaskBehavior(new TaskPredictionInput(someJob.getId(), TaskType.REDUCE));
    assertThat(prediction.getDuration(), is(66321L));

    someJob.setAvgReduceDuration(1001L);
    prediction = subject.predictTaskBehavior(new TaskPredictionInput(someJob.getId(), TaskType.REDUCE));
    assertThat(prediction.getDuration(), is(1001L));

    prediction = subject.predictTaskBehavior(new TaskPredictionInput(anotherJob.getId(), TaskType.REDUCE));
    assertThat(prediction.getDuration(), is(DEFAULT_TASK_DURATION));

    anotherJob.setAvgReduceDuration(2001L);
    prediction = subject.predictTaskBehavior(new TaskPredictionInput(anotherJob.getId(), TaskType.REDUCE));
    assertThat(prediction.getDuration(), is(2001L));
  }
}
