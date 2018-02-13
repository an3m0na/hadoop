package org.apache.hadoop.tools.posum.simulation.predictor.standard;

import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.tools.posum.common.records.call.IdsByQueryCall;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.TaskProfile;
import org.apache.hadoop.tools.posum.simulation.predictor.TaskPredictionInput;
import org.apache.hadoop.tools.posum.simulation.predictor.TaskPredictionOutput;
import org.apache.hadoop.tools.posum.simulation.predictor.TestPredictor;
import org.junit.Test;

import java.util.List;
import java.util.Set;

import static org.apache.hadoop.mapreduce.v2.api.records.TaskType.MAP;
import static org.apache.hadoop.mapreduce.v2.api.records.TaskType.REDUCE;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.JOB_HISTORY;
import static org.apache.hadoop.tools.posum.simulation.predictor.simple.SimpleStatKeys.MAP_DURATION;
import static org.apache.hadoop.tools.posum.simulation.predictor.simple.SimpleStatKeys.REDUCE_DURATION;
import static org.apache.hadoop.tools.posum.simulation.predictor.standard.StandardStatKeys.MAP_RATE;
import static org.apache.hadoop.tools.posum.simulation.predictor.standard.StandardStatKeys.MAP_SELECTIVITY;
import static org.apache.hadoop.tools.posum.simulation.predictor.standard.StandardStatKeys.REDUCE_RATE;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

public class TestStandardPredictor extends TestPredictor<StandardPredictor> {
  public TestStandardPredictor() {
    super(StandardPredictor.class);
  }

  @Test
  public void testModelMakeup() {
    Set<String> sourceJobs = predictor.getModel().getSourceJobs();
    List<String> historicalJobs = db.execute(IdsByQueryCall.newInstance(JOB_HISTORY, null)).getEntries();
    assertThat(sourceJobs, containsInAnyOrder(historicalJobs.toArray()));

    JobProfile knownUserJob = createJobOnKnownUser();
    JobProfile unknownUserJob = createJobOnUnknownUser();

    // check history stats for known user & unknown map/reduce classes
    StandardMapPredictionStats knownUserJobMapStats = predictor.getModel().getRelevantMapStats(knownUserJob);
    StandardReducePredictionStats knownUserJobReduceStats = predictor.getModel().getRelevantReduceStats(knownUserJob);
    assertThat(knownUserJobMapStats.getEntry(MAP_DURATION).getAverage(), is(3652.267932489452));
    assertThat(knownUserJobMapStats.getEntry(MAP_RATE).getAverage(), is(17477.210291517793));
    assertThat(knownUserJobMapStats.getEntry(MAP_SELECTIVITY).getAverage(), is(0.8537658416558236));
    assertThat(knownUserJobReduceStats.getEntry(REDUCE_DURATION).getAverage(), is(38556.70000000001));
    assertThat(knownUserJobReduceStats.getEntry(REDUCE_RATE).getAverage(), is(48446.2100838046));

    // check history stats for known map/reduce classes
    knownUserJob.setMapperClass("org.apache.hadoop.mapreduce.lib.map.RegexMapper");
    knownUserJob.setReducerClass("org.apache.nutch.indexer.IndexerMapReduce");
    knownUserJobMapStats = predictor.getModel().getRelevantMapStats(knownUserJob);
    knownUserJobReduceStats = predictor.getModel().getRelevantReduceStats(knownUserJob);
    assertThat(knownUserJobMapStats.getEntry(MAP_DURATION).getAverage(), is(4042.3125));
    assertThat(knownUserJobMapStats.getEntry(MAP_RATE).getAverage(), is(16299.558275983596));
    assertThat(knownUserJobMapStats.getEntry(MAP_SELECTIVITY).getAverage(), is(9.123336002731678E-8));
    assertThat(knownUserJobReduceStats.getEntry(REDUCE_DURATION).getAverage(), is(126436.0));
    assertThat(knownUserJobReduceStats.getEntry(REDUCE_RATE).getAverage(), is(8426.27315355946));

    // check history stats for unknown user
    StandardMapPredictionStats unknownUserJobMapStats = predictor.getModel().getRelevantMapStats(unknownUserJob);
    StandardReducePredictionStats unknownUserJobReduceStats = predictor.getModel().getRelevantReduceStats(unknownUserJob);
    assertThat(unknownUserJobMapStats, nullValue());
    assertThat(unknownUserJobReduceStats, nullValue());
  }

  @Test
  public void testMapPrediction() {
    JobProfile knownUserJob = createJobOnKnownUser();
    JobProfile unknownUserJob = createJobOnUnknownUser();

    // check prediction for no info
    TaskPredictionOutput prediction = predictor.predictTaskBehavior(new TaskPredictionInput(unknownUserJob.getId(), MAP));
    assertThat(prediction.getDuration(), is(DEFAULT_TASK_DURATION));

    // check prediction when rate calculation is not possible, but general average duration is known
    prediction = predictor.predictTaskBehavior(new TaskPredictionInput(knownUserJob, MAP));
    assertThat(prediction.getDuration(), is(3652L));

    // check prediction when rate calculation is not possible, but job's average duration is known
    TaskProfile mapTask = getTaskForJob(knownUserJob.getId(), MAP);
    mapTask.setStartTime(0L);
    mapTask.setFinishTime(500L);
    knownUserJob.setCompletedMaps(1);
    save(mapTask);
    prediction = predictor.predictTaskBehavior(new TaskPredictionInput(knownUserJob, MAP));
    assertThat(prediction.getDuration(), is(500L));

    // check prediction when rate calculation is not possible, but class average duration is known
    knownUserJob.setMapperClass("org.apache.hadoop.mapreduce.lib.map.RegexMapper");
    prediction = predictor.predictTaskBehavior(new TaskPredictionInput(knownUserJob, MAP));
    assertThat(prediction.getDuration(), is(4042L));

    // check rate-based prediction for class average rate
    knownUserJob.setTotalSplitSize(100_000_000L);
    knownUserJob.setTotalMapTasks(10);
    prediction = predictor.predictTaskBehavior(new TaskPredictionInput(knownUserJob, MAP));
    assertThat(prediction.getDuration(), is(613L));

    // check rate-based prediction for general average rate
    knownUserJob.setMapperClass(null);
    prediction = predictor.predictTaskBehavior(new TaskPredictionInput(knownUserJob, MAP));
    assertThat(prediction.getDuration(), is(572L));

    // trigger job stat recalculation and check rate-based prediction on job rate
    knownUserJob.setCompletedMaps(2);
    mapTask.setSplitSize(10_000L);
    save(mapTask);
    prediction = predictor.predictTaskBehavior(new TaskPredictionInput(knownUserJob, MAP));
    assertThat(prediction.getDuration(), is(500_000L));
  }

  @Test
  public void testReducePrediction() {
    JobProfile knownUserJob = createJobOnKnownUser();
    JobProfile unknownUserJob = createJobOnUnknownUser();

    // check prediction for no info
    TaskPredictionOutput prediction = predictor.predictTaskBehavior(new TaskPredictionInput(unknownUserJob.getId(), TaskType.REDUCE));
    assertThat(prediction.getDuration(), is(DEFAULT_TASK_DURATION));

    // check rate-based prediction with general reduce rate and selectivity
    knownUserJob.setTotalSplitSize(100_000_000L);
    prediction = predictor.predictTaskBehavior(new TaskPredictionInput(knownUserJob, TaskType.REDUCE));
    assertThat(prediction.getDuration(), is(1762L));

    // check prediction for relevant average duration
    TaskProfile reduceTask = getTaskForJob(knownUserJob.getId(), REDUCE);
    reduceTask.setStartTime(0L);
    reduceTask.setFinishTime(500L);
    knownUserJob.setCompletedReduces(1);
    save(reduceTask);
    prediction = predictor.predictTaskBehavior(new TaskPredictionInput(knownUserJob, TaskType.REDUCE));
    assertThat(prediction.getDuration(), is(500L));

    // trigger job stat recalculation and check rate-based prediction for job's stats
    TaskProfile mapTask = getTaskForJob(knownUserJob.getId(), MAP);
    mapTask.setStartTime(0L);
    mapTask.setFinishTime(10L);
    mapTask.setSplitSize(2_000_000L);
    mapTask.setOutputBytes(50_000L);
    knownUserJob.setCompletedMaps(1);
    save(mapTask);
    reduceTask.setInputBytes(100L);
    knownUserJob.setTotalReduceTasks(10);
    knownUserJob.setCompletedReduces(2);
    save(reduceTask);
    prediction = predictor.predictTaskBehavior(new TaskPredictionInput(knownUserJob, TaskType.REDUCE));
    assertThat(prediction.getDuration(), is(1562500L));

    // check rate-based prediction for class reduce rate
    knownUserJob.setReducerClass("org.apache.nutch.indexer.IndexerMapReduce");
    prediction = predictor.predictTaskBehavior(new TaskPredictionInput(knownUserJob, TaskType.REDUCE));
    assertThat(prediction.getDuration(), is(37L));
  }
}
