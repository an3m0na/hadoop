package org.apache.hadoop.tools.posum.common.util.cluster;

import org.apache.hadoop.tools.posum.client.data.Database;
import org.apache.hadoop.tools.posum.client.data.DatabaseUtils;
import org.apache.hadoop.tools.posum.common.records.call.FindByQueryCall;
import org.apache.hadoop.tools.posum.common.records.call.query.QueryUtils;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.TaskProfile;
import org.apache.hadoop.tools.posum.common.records.payload.CompoundScorePayload;
import org.apache.hadoop.tools.posum.common.records.payload.MultiEntityPayload;
import org.apache.hadoop.yarn.util.Records;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestPerformanceEvaluator {
  private List<JobProfile> JOBS = Arrays.asList(
    Records.newRecord(JobProfile.class),
    Records.newRecord(JobProfile.class),
    Records.newRecord(JobProfile.class),
    Records.newRecord(JobProfile.class)
  );

  private List<TaskProfile> TASKS0 = Arrays.asList(
    Records.newRecord(TaskProfile.class),
    Records.newRecord(TaskProfile.class)
  );

  private List<TaskProfile> TASKS2 = Arrays.asList(
    Records.newRecord(TaskProfile.class),
    Records.newRecord(TaskProfile.class)
  );

  @Mock
  private Database db;

  @Before
  public void setUp() throws Exception {
    JOBS.get(0).setId("0");
    JOBS.get(0).setStartTime(100L);
    JOBS.get(0).setFinishTime(300L);
    JOBS.get(1).setId("1");
    JOBS.get(1).setStartTime(200L);
    JOBS.get(1).setFinishTime(600L);
    JOBS.get(2).setId("2");
    JOBS.get(2).setStartTime(250000L);
    JOBS.get(2).setFinishTime(550000L);
    JOBS.get(3).setId("3");
    JOBS.get(3).setStartTime(321L);
    JOBS.get(3).setFinishTime(415L);

    TASKS0.get(0).setJobId("0");
    TASKS0.get(0).setStartTime(10L);
    TASKS0.get(0).setFinishTime(30L);
    TASKS0.get(1).setJobId("0");
    TASKS0.get(1).setStartTime(20L);
    TASKS0.get(1).setFinishTime(60L);

    TASKS2.get(0).setJobId("2");
    TASKS2.get(0).setStartTime(25000L);
    TASKS2.get(0).setFinishTime(55000L);
    TASKS2.get(1).setId("2");
    TASKS2.get(1).setStartTime(32000L);
    TASKS2.get(1).setFinishTime(41000L);

    when(db.execute(FindByQueryCall.newInstance(DataEntityCollection.JOB_HISTORY, null)))
      .thenReturn(MultiEntityPayload.newInstance(DataEntityCollection.JOB_HISTORY, JOBS));
    when(db.execute(FindByQueryCall.newInstance(DataEntityCollection.TASK_HISTORY,
      QueryUtils.is("jobId", "0"))))
      .thenReturn(MultiEntityPayload.newInstance(DataEntityCollection.TASK_HISTORY, TASKS0));
    when(db.execute(FindByQueryCall.newInstance(DataEntityCollection.TASK_HISTORY,
      QueryUtils.is("jobId", "2"))))
      .thenReturn(MultiEntityPayload.newInstance(DataEntityCollection.TASK_HISTORY, TASKS2));
  }

  @Test
  public void testEvaluationForExpired() {
    JOBS.get(1).setDeadline(500L);
    JOBS.get(3).setDeadline(30L);
    CompoundScorePayload result = new PerformanceEvaluator(DatabaseUtils.newProvider(db)).evaluate();
    assertThat(result, is(CompoundScorePayload.newInstance(7.712307692307692, 0.28126944377233726, 0.0)));
  }

  @Test
  public void testEvaluationForNotExpired() {
    JOBS.get(1).setDeadline(700L);
    JOBS.get(3).setDeadline(550L);
    CompoundScorePayload result = new PerformanceEvaluator(DatabaseUtils.newProvider(db)).evaluate();
    assertThat(result, is(CompoundScorePayload.newInstance(7.712307692307692, 0.0, 0.0)));
  }
}
