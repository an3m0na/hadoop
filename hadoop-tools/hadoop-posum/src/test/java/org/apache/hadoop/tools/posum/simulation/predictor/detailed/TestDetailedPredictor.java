package org.apache.hadoop.tools.posum.simulation.predictor.detailed;

import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.tools.posum.common.records.call.IdsByQueryCall;
import org.apache.hadoop.tools.posum.common.records.call.UpdateOrStoreCall;
import org.apache.hadoop.tools.posum.simulation.predictor.TaskPredictionInput;
import org.apache.hadoop.tools.posum.simulation.predictor.TaskPredictionOutput;
import org.apache.hadoop.tools.posum.simulation.predictor.TestPredictor;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.JOB;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.JOB_HISTORY;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.TASK;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

public class TestDetailedPredictor extends TestPredictor<DetailedPredictor> {
  public TestDetailedPredictor() {
    super(DetailedPredictor.class);
  }

  @Test
  public void testModelMakeup() throws Exception {
    Set<String> sourceJobs = predictor.getModel().getSourceJobs();
    List<String> historicalJobs = db.execute(IdsByQueryCall.newInstance(JOB_HISTORY, null)).getEntries();
    assertThat(sourceJobs, containsInAnyOrder(historicalJobs.toArray()));

    // check history stats for known user & unknown map/reduce classes
    DetailedMapPredictionStats someJobMapStats = predictor.getModel().getRelevantMapStats(someJob);
    DetailedReducePredictionStats someJobReduceStats = predictor.getModel().getRelevantReduceStats(someJob);
    assertThat(someJobMapStats.getAvgRate(), closeTo(15891.11, 0.01));
    assertThat(someJobMapStats.getAvgSelectivity(), closeTo(1.361, 0.001));
    assertThat(someJobMapStats.getAvgLocalRate(), closeTo(16086.84, 0.01));
    assertThat(someJobMapStats.getAvgRemoteRate(), closeTo(14630.82, 0.01));
    assertThat(someJobReduceStats.getAvgReduceDuration(), closeTo(82522.0, 0.1));
    assertThat(someJobReduceStats.getAvgReduceRate(), closeTo(161242.10, 0.01));
    assertThat(someJobReduceStats.getAvgMergeRate(), closeTo(2463112.18, 0.01));
    assertThat(someJobReduceStats.getAvgShuffleFirstTime(), closeTo(3345.0, 0.1));
    assertThat(someJobReduceStats.getAvgShuffleTypicalRate(), closeTo(116483.44, 0.01));

    // check history stats for known map/reduce classes
    someJob.setMapperClass("org.apache.hadoop.mapreduce.lib.map.RegexMapper");
    someJob.setReducerClass("org.apache.nutch.indexer.IndexerMapReduce");
    someJobMapStats = predictor.getModel().getRelevantMapStats(someJob);
    someJobReduceStats = predictor.getModel().getRelevantReduceStats(someJob);
    assertThat(someJobMapStats.getAvgRate(), closeTo(16268.63, 0.01));
    assertThat(someJobMapStats.getAvgSelectivity(), closeTo(9.123E-8, 0.001E-8));
    assertThat(someJobMapStats.getAvgLocalRate(), closeTo(16253.48, 0.01));
    assertThat(someJobMapStats.getAvgRemoteRate(), closeTo(16314.75, 0.01));
    assertThat(someJobReduceStats.getAvgReduceDuration(), closeTo(129203.0, 0.1));
    assertThat(someJobReduceStats.getAvgReduceRate(), closeTo(9920.24, 0.01));
    assertThat(someJobReduceStats.getAvgMergeRate(), closeTo(3317439.20, 0.01));
    assertThat(someJobReduceStats.getAvgShuffleFirstTime(), closeTo(658.0, 0.01));
    assertThat(someJobReduceStats.getAvgShuffleTypicalRate(), nullValue());

    // check history stats for unknown user
    DetailedMapPredictionStats anotherJobMapStats = predictor.getModel().getRelevantMapStats(anotherJob);
    DetailedReducePredictionStats anotherJobReduceStats = predictor.getModel().getRelevantReduceStats(anotherJob);
    assertThat(anotherJobMapStats, nullValue());
    assertThat(anotherJobReduceStats, nullValue());
  }

  @Test
  public void testMapPrediction() throws Exception {
    TaskPredictionOutput prediction = predictor.predictTaskBehavior(new TaskPredictionInput(someJob, TaskType.MAP));
    // check prediction for missing input size -> default
    assertThat(prediction.getDuration(), is(1500L));

    // check rate-based prediction
    someJob.setTotalInputBytes(100000000L);
    someJob.setTotalMapTasks(10);
    prediction = predictor.predictTaskBehavior(new TaskPredictionInput(someJob, TaskType.MAP));
    assertThat(prediction.getDuration(), is(629L));

    // check rate-based prediction for local task
    someJobMapTask.setSplitLocations(Collections.singletonList("bla"));
    db.execute(UpdateOrStoreCall.newInstance(TASK, someJobMapTask));
    db.execute(UpdateOrStoreCall.newInstance(JOB, someJob));
    prediction = predictor.predictTaskBehavior(new TaskPredictionInput(someJobMapTask.getId(), "bla"));
    assertThat(prediction.getDuration(), is(621L));

    // check rate-based prediction for remote task
    prediction = predictor.predictTaskBehavior(new TaskPredictionInput(someJobMapTask.getId(), "ha"));
    assertThat(prediction.getDuration(), is(683L));

    // check prediction on known average duration and irrelevant history
    someJob.setAvgMapDuration(1000L);
    prediction = predictor.predictTaskBehavior(new TaskPredictionInput(someJob, TaskType.MAP));
    assertThat(prediction.getDuration(), is(1000L));

    // check prediction for relevant history
    someJob.setMapperClass("org.apache.hadoop.mapreduce.lib.map.RegexMapper");
    prediction = predictor.predictTaskBehavior(new TaskPredictionInput(someJob, TaskType.MAP));
    assertThat(prediction.getDuration(), is(614L));

    // check prediction for relevant history and known locality
    prediction = predictor.predictTaskBehavior(new TaskPredictionInput(someJobMapTask.getId(), "ha"));
    assertThat(prediction.getDuration(), is(612L));

    // check no history prediction
    prediction = predictor.predictTaskBehavior(new TaskPredictionInput(anotherJob, TaskType.MAP));
    assertThat(prediction.getDuration(), is(DEFAULT_TASK_DURATION));

    // check no history but known average prediction
    anotherJob.setAvgMapDuration(2000L);
    prediction = predictor.predictTaskBehavior(new TaskPredictionInput(anotherJob, TaskType.MAP));
    assertThat(prediction.getDuration(), is(2000L));
  }

  @Test
  public void testReducePrediction() throws Exception {
    // check prediction with no selectivity data -> avg historical duration
    someJob.setTotalInputBytes(100000000L);
    someJob.setTotalMapTasks(10);
    someJob.setTotalReduceTasks(2);
    TaskPredictionOutput prediction = predictor.predictTaskBehavior(new TaskPredictionInput(someJob, TaskType.REDUCE));
    assertThat(prediction.getDuration(), is(82522L));

    // check rate-based prediction with own selectivity data
    someJob.setMapOutputBytes(100L);
    someJobMapTask.setSplitSize(4L);
    someJobMapTask.setStartTime(1000L);
    someJobMapTask.setFinishTime(2000L);
    db.execute(UpdateOrStoreCall.newInstance(TASK, someJobMapTask));
    someJob.setCompletedMaps(1);
    prediction = predictor.predictTaskBehavior(new TaskPredictionInput(someJob, TaskType.REDUCE));
    assertThat(prediction.getDuration(), is(18990L));

    // check rate-based prediction with historical selectivity data
    someJob.setMapperClass("org.apache.nutch.indexer.IndexerMapReduce");
    prediction = predictor.predictTaskBehavior(new TaskPredictionInput(someJob, TaskType.REDUCE));
    assertThat(prediction.getDuration(), is(1309L));

    // check prediction for known avg duration and history
    someJob.setAvgReduceDuration(1001L);
    prediction = predictor.predictTaskBehavior(new TaskPredictionInput(someJob, TaskType.REDUCE));
    assertThat(prediction.getDuration(), is(1309L));

    // check prediction for restricted reduce info
    someJob.setReducerClass("org.apache.nutch.indexer.IndexerMapReduce");
    someJob.setAvgReduceDuration(1001L);
    prediction = predictor.predictTaskBehavior(new TaskPredictionInput(someJob, TaskType.REDUCE));
    assertThat(prediction.getDuration(), is(1001L));

    // check prediction for no data
    prediction = predictor.predictTaskBehavior(new TaskPredictionInput(anotherJob, TaskType.REDUCE));
    assertThat(prediction.getDuration(), is(DEFAULT_TASK_DURATION));

    // check prediction for no history, but known avg duration
    anotherJob.setAvgReduceDuration(2001L);
    prediction = predictor.predictTaskBehavior(new TaskPredictionInput(anotherJob, TaskType.REDUCE));
    assertThat(prediction.getDuration(), is(2001L));
  }
}
