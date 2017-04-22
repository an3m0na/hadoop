package org.apache.hadoop.tools.posum.simulation.predictor.basic;

import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.tools.posum.common.records.call.IdsByQueryCall;
import org.apache.hadoop.tools.posum.simulation.predictor.TaskPredictionInput;
import org.apache.hadoop.tools.posum.simulation.predictor.TaskPredictionOutput;
import org.apache.hadoop.tools.posum.simulation.predictor.TestPredictor;
import org.junit.Test;

import java.util.List;
import java.util.Set;

import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.JOB_HISTORY;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

public class TestBasicPredictor extends TestPredictor<BasicPredictor> {
  
  public TestBasicPredictor() {
    super(BasicPredictor.class);
  }
  
  @Test
  public void testModelMakeup() throws Exception {
    Set<String> sourceJobs = predictor.getModel().getSourceJobs();
    List<String> historicalJobs = db.execute(IdsByQueryCall.newInstance(JOB_HISTORY, null)).getEntries();
    assertThat(sourceJobs, containsInAnyOrder(historicalJobs.toArray()));

    // check history stats for known user
    BasicPredictionStats someJobStats = predictor.getModel().getRelevantStats(someJob);
    assertThat(someJobStats.getAvgMapDuration(), is(2801.0));
    assertThat(someJobStats.getAvgReduceDuration(), is(66321.0));

    // check history stats for unknown user
    BasicPredictionStats anotherJobStats = predictor.getModel().getRelevantStats(anotherJob);
    assertThat(anotherJobStats, nullValue());
  }

  @Test
  public void testMapPrediction() throws Exception {
    TaskPredictionOutput prediction = predictor.predictTaskBehavior(new TaskPredictionInput(someJob.getId(), TaskType.MAP));
    assertThat(prediction.getDuration(), is(2801L));

    someJob.setAvgMapDuration(1000L);
    prediction = predictor.predictTaskBehavior(new TaskPredictionInput(someJob.getId(), TaskType.MAP));
    assertThat(prediction.getDuration(), is(1000L));

    prediction = predictor.predictTaskBehavior(new TaskPredictionInput(anotherJob.getId(), TaskType.MAP));
    assertThat(prediction.getDuration(), is(DEFAULT_TASK_DURATION));

    anotherJob.setAvgMapDuration(2000L);
    prediction = predictor.predictTaskBehavior(new TaskPredictionInput(anotherJob.getId(), TaskType.MAP));
    assertThat(prediction.getDuration(), is(2000L));
  }

  @Test
  public void testReducePrediction() throws Exception {
    TaskPredictionOutput prediction = predictor.predictTaskBehavior(new TaskPredictionInput(someJob.getId(), TaskType.REDUCE));
    assertThat(prediction.getDuration(), is(66321L));

    someJob.setAvgReduceDuration(1001L);
    prediction = predictor.predictTaskBehavior(new TaskPredictionInput(someJob.getId(), TaskType.REDUCE));
    assertThat(prediction.getDuration(), is(1001L));

    prediction = predictor.predictTaskBehavior(new TaskPredictionInput(anotherJob.getId(), TaskType.REDUCE));
    assertThat(prediction.getDuration(), is(DEFAULT_TASK_DURATION));

    anotherJob.setAvgReduceDuration(2001L);
    prediction = predictor.predictTaskBehavior(new TaskPredictionInput(anotherJob.getId(), TaskType.REDUCE));
    assertThat(prediction.getDuration(), is(2001L));
  }
}
