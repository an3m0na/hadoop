package org.apache.hadoop.tools.posum.simulation.predictor.basic;

import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.tools.posum.common.records.call.IdsByQueryCall;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.simulation.predictor.TaskPredictionInput;
import org.apache.hadoop.tools.posum.simulation.predictor.TaskPredictionOutput;
import org.apache.hadoop.tools.posum.simulation.predictor.TestPredictor;
import org.junit.Test;

import java.util.List;
import java.util.Set;

import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.JOB_HISTORY;
import static org.apache.hadoop.tools.posum.simulation.predictor.simple.SimpleStatKeys.MAP_DURATION;
import static org.apache.hadoop.tools.posum.simulation.predictor.simple.SimpleStatKeys.REDUCE_DURATION;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

public class TestBasicPredictor extends TestPredictor<BasicPredictor> {

  public TestBasicPredictor() {
    super(BasicPredictor.class);
  }

  @Test
  public void testModelMakeup() {
    Set<String> sourceJobs = predictor.getModel().getSourceJobs();
    List<String> historicalJobs = db.execute(IdsByQueryCall.newInstance(JOB_HISTORY, null)).getEntries();
    assertThat(sourceJobs, containsInAnyOrder(historicalJobs.toArray()));

    // check history stats for known user
    BasicPredictionStats knownUserJobStats = predictor.getModel().getRelevantStats(createJobOnKnownUser());
    assertThat(knownUserJobStats.getAverage(MAP_DURATION), is(3651.8755274261603));
    assertThat(knownUserJobStats.getAverage(REDUCE_DURATION), is(38555.925));

    // check history stats for unknown user
    BasicPredictionStats unknownUserJobStats = predictor.getModel().getRelevantStats(createJobOnUnknownUser());
    assertThat(unknownUserJobStats, nullValue());
  }

  @Test
  public void testMapPrediction() {
    JobProfile knownUserJob = createJobOnKnownUser();
    JobProfile unknownUserJob = createJobOnUnknownUser();
    TaskPredictionOutput prediction = predictor.predictTaskBehavior(new TaskPredictionInput(knownUserJob.getId(), TaskType.MAP));
    assertThat(prediction.getDuration(), is(3651L));

    knownUserJob.setAvgMapDuration(1000L);
    prediction = predictor.predictTaskBehavior(new TaskPredictionInput(knownUserJob, TaskType.MAP));
    assertThat(prediction.getDuration(), is(1000L));

    prediction = predictor.predictTaskBehavior(new TaskPredictionInput(unknownUserJob, TaskType.MAP));
    assertThat(prediction.getDuration(), is(DEFAULT_TASK_DURATION));

    unknownUserJob.setAvgMapDuration(2000L);
    prediction = predictor.predictTaskBehavior(new TaskPredictionInput(unknownUserJob, TaskType.MAP));
    assertThat(prediction.getDuration(), is(2000L));
  }

  @Test
  public void testReducePrediction() {
    JobProfile knownUserJob = createJobOnKnownUser();
    JobProfile unknownUserJob = createJobOnUnknownUser();
    TaskPredictionOutput prediction = predictor.predictTaskBehavior(new TaskPredictionInput(knownUserJob.getId(), TaskType.REDUCE));
    assertThat(prediction.getDuration(), is(38555L));

    knownUserJob.setAvgReduceDuration(1001L);
    prediction = predictor.predictTaskBehavior(new TaskPredictionInput(knownUserJob, TaskType.REDUCE));
    assertThat(prediction.getDuration(), is(1001L));

    prediction = predictor.predictTaskBehavior(new TaskPredictionInput(unknownUserJob, TaskType.REDUCE));
    assertThat(prediction.getDuration(), is(DEFAULT_TASK_DURATION));

    unknownUserJob.setAvgReduceDuration(2001L);
    prediction = predictor.predictTaskBehavior(new TaskPredictionInput(unknownUserJob, TaskType.REDUCE));
    assertThat(prediction.getDuration(), is(2001L));
  }
}
