package org.apache.hadoop.tools.posum.simulation.predictor.detailed;

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
import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.DetailedStatKeys.MAP_LOCAL_RATE;
import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.DetailedStatKeys.MAP_RATE;
import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.DetailedStatKeys.MAP_REMOTE_RATE;
import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.DetailedStatKeys.MAP_SELECTIVITY;
import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.DetailedStatKeys.MERGE_RATE;
import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.DetailedStatKeys.REDUCE_RATE;
import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.DetailedStatKeys.SHUFFLE_FIRST_DURATION;
import static org.apache.hadoop.tools.posum.simulation.predictor.detailed.DetailedStatKeys.SHUFFLE_TYPICAL_RATE;
import static org.apache.hadoop.tools.posum.simulation.predictor.simple.SimpleStatKeys.MAP_DURATION;
import static org.apache.hadoop.tools.posum.simulation.predictor.simple.SimpleStatKeys.REDUCE_DURATION;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

public class TestDetailedPredictor extends TestPredictor<DetailedPredictor> {
  public TestDetailedPredictor() {
    super(DetailedPredictor.class);
  }

  @Test
  public void testModelMakeup() {
    Set<String> sourceJobs = predictor.getModel().getSourceJobs();
    List<String> historicalJobs = db.execute(IdsByQueryCall.newInstance(JOB_HISTORY, null)).getEntries();
    assertThat(sourceJobs, containsInAnyOrder(historicalJobs.toArray()));

    JobProfile knownUserJob = createJobOnKnownUser();
    JobProfile unknownUserJob = createJobOnUnknownUser();

    // check history stats for known user & unknown map/reduce classes
    DetailedMapPredictionStats knownUserJobMapStats = predictor.getModel().getRelevantMapStats(knownUserJob);
    DetailedReducePredictionStats knownUserJobReduceStats = predictor.getModel().getRelevantReduceStats(knownUserJob);
    assertThat(knownUserJobMapStats.getEntry(MAP_DURATION).getAverage(), is(3652.267932489452));
    assertThat(knownUserJobMapStats.getEntry(MAP_RATE).getAverage(), is(17477.210291517793));
    assertThat(knownUserJobMapStats.getEntry(MAP_SELECTIVITY).getAverage(), is(0.8537658416558236));
    assertThat(knownUserJobMapStats.getEntry(MAP_LOCAL_RATE).getAverage(), is(17994.40641603625));
    assertThat(knownUserJobMapStats.getEntry(MAP_REMOTE_RATE).getAverage(), is(18077.198895748967));
    assertThat(knownUserJobReduceStats.getEntry(REDUCE_DURATION).getAverage(), is(38556.70000000001));
    assertThat(knownUserJobReduceStats.getEntry(REDUCE_RATE).getAverage(), is(204901.1652928512));
    assertThat(knownUserJobReduceStats.getEntry(MERGE_RATE).getAverage(), is(1856219.125144537));
    assertThat(knownUserJobReduceStats.getEntry(SHUFFLE_FIRST_DURATION).getAverage(), is(1340.6));
    assertThat(knownUserJobReduceStats.getEntry(SHUFFLE_TYPICAL_RATE).getAverage(), is(103647.2241162516));

    // check history stats for known map/reduce classes
    knownUserJob.setMapperClass("org.apache.hadoop.mapreduce.lib.map.RegexMapper");
    knownUserJob.setReducerClass("org.apache.nutch.indexer.IndexerMapReduce");
    knownUserJobMapStats = predictor.getModel().getRelevantMapStats(knownUserJob);
    knownUserJobReduceStats = predictor.getModel().getRelevantReduceStats(knownUserJob);
    assertThat(knownUserJobMapStats.getEntry(MAP_DURATION).getAverage(), is(4042.3125));
    assertThat(knownUserJobMapStats.getEntry(MAP_RATE).getAverage(), is(16299.558275983596));
    assertThat(knownUserJobMapStats.getEntry(MAP_SELECTIVITY).getAverage(), is(9.123336002731678E-8));
    assertThat(knownUserJobMapStats.getEntry(MAP_LOCAL_RATE).getAverage(), is(16290.30324995309));
    assertThat(knownUserJobMapStats.getEntry(MAP_REMOTE_RATE), nullValue());
    assertThat(knownUserJobReduceStats.getEntry(REDUCE_DURATION).getAverage(), is(126436.0));
    assertThat(knownUserJobReduceStats.getEntry(REDUCE_RATE).getAverage(), is(9914.625746704905));
    assertThat(knownUserJobReduceStats.getEntry(MERGE_RATE).getAverage(), is(4101254.945479382));
    assertThat(knownUserJobReduceStats.getEntry(SHUFFLE_FIRST_DURATION).getAverage(), is(613.6));
    assertThat(knownUserJobReduceStats.getEntry(SHUFFLE_TYPICAL_RATE), nullValue());

    // check history stats for unknown user
    DetailedMapPredictionStats unknownUserJobMapStats = predictor.getModel().getRelevantMapStats(unknownUserJob);
    DetailedReducePredictionStats unknownUserJobReduceStats = predictor.getModel().getRelevantReduceStats(unknownUserJob);
    assertThat(unknownUserJobMapStats, nullValue());
    assertThat(unknownUserJobReduceStats, nullValue());
  }

  @Test
  public void testMapPrediction() {
    JobProfile knownUserJob = createJobOnKnownUser();
    JobProfile unknownUserJob = createJobOnUnknownUser();

    TaskPredictionOutput prediction = predictor.predictTaskBehavior(new TaskPredictionInput(unknownUserJob.getId(), TaskType.MAP));
    // check prediction for no info
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

    // check rate-based prediction for class average rate with specific locality
    prediction = predictor.predictTaskBehavior(new DetailedTaskPredictionInput(knownUserJob, MAP, true));
    assertThat(prediction.getDuration(), is(613L));

    // check rate-based prediction for general average rate
    knownUserJob.setMapperClass(null);
    prediction = predictor.predictTaskBehavior(new TaskPredictionInput(knownUserJob, MAP));
    assertThat(prediction.getDuration(), is(572L));

    // trigger job stat recalculation and check rate-based prediction on job rate and specific locality
    mapTask.setSplitSize(10_000L);
    save(mapTask);
    TaskProfile newMapTask = addTask(knownUserJob.getId(), MAP);
    newMapTask.setSplitSize(10_000L);
    newMapTask.setStartTime(0L);
    newMapTask.setFinishTime(5000L);
    newMapTask.setLocal(true);
    save(newMapTask);
    knownUserJob.setCompletedMaps(2);
    prediction = predictor.predictTaskBehavior(new DetailedTaskPredictionInput(knownUserJob, MAP, false));
    // should use general rate
    assertThat(prediction.getDuration(), is(909_090L));
    prediction = predictor.predictTaskBehavior(new DetailedTaskPredictionInput(knownUserJob, MAP, true));
    // should use local rate
    assertThat(prediction.getDuration(), is(5_000_000L));
  }

  @Test
  public void testReducePrediction() {
    JobProfile knownUserJob = createJobOnKnownUser();
    JobProfile unknownUserJob = createJobOnUnknownUser();

    // check prediction for no info
    TaskPredictionOutput prediction = predictor.predictTaskBehavior(new TaskPredictionInput(unknownUserJob.getId(), REDUCE));
    assertThat(prediction.getDuration(), is(DEFAULT_TASK_DURATION));

    // check rate-based prediction with general map rate and selectivity
    knownUserJob.setTotalSplitSize(100_000_000L);
    prediction = predictor.predictTaskBehavior(new TaskPredictionInput(knownUserJob, REDUCE));
    assertThat(prediction.getDuration(), is(4885L));

    // check prediction for relevant average duration
    TaskProfile reduceTask = getTaskForJob(knownUserJob.getId(), REDUCE);
    reduceTask.setStartTime(0L);
    reduceTask.setFinishTime(500L);
    knownUserJob.setCompletedReduces(1);
    save(reduceTask);
    prediction = predictor.predictTaskBehavior(new TaskPredictionInput(knownUserJob, REDUCE));
    assertThat(prediction.getDuration(), is(500L));

    // trigger job stat recalculation and check rate-based prediction for job's stats
    TaskProfile mapTask = getTaskForJob(knownUserJob.getId(), MAP);
    mapTask.setStartTime(0L);
    mapTask.setFinishTime(10L);
    mapTask.setSplitSize(2_000_000L);
    mapTask.setOutputBytes(50_000L);
    knownUserJob.setTotalMapTasks(2);
    knownUserJob.setCompletedMaps(1);
    save(mapTask);
    reduceTask.setInputBytes(100L);
    reduceTask.setShuffleTime(300L);
    reduceTask.setMergeTime(30L);
    reduceTask.setReduceTime(170L);
    knownUserJob.setTotalReduceTasks(10);
    knownUserJob.setCompletedReduces(2);
    save(reduceTask);
    prediction = predictor.predictTaskBehavior(new TaskPredictionInput(knownUserJob, REDUCE));
    assertThat(prediction.getDuration(), is(625290L));

    // check rate-based prediction for class reduce stats and first shuffle
    knownUserJob.setReducerClass("org.apache.hadoop.mapreduce.Reducer");
    prediction = predictor.predictTaskBehavior(new TaskPredictionInput(knownUserJob, REDUCE));
    assertThat(prediction.getDuration(), is(2860L));

    // check rate-based prediction for class reduce stats and typical shuffle
    knownUserJob.setReducerClass("org.apache.hadoop.mapreduce.Reducer");
    knownUserJob.setCompletedMaps(2);
    prediction = predictor.predictTaskBehavior(new TaskPredictionInput(knownUserJob, REDUCE));
    assertThat(prediction.getDuration(), is(3L));
  }
}
