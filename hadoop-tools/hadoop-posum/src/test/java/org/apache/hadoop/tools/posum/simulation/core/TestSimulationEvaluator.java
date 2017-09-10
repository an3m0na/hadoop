package org.apache.hadoop.tools.posum.simulation.core;

import org.apache.hadoop.tools.posum.client.data.Database;
import org.apache.hadoop.tools.posum.common.records.call.FindByQueryCall;
import org.apache.hadoop.tools.posum.common.records.call.query.QueryUtils;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobConfProxy;
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
import java.util.Collections;
import java.util.List;

import static org.apache.hadoop.tools.posum.common.util.PosumConfiguration.APP_DEADLINE;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestSimulationEvaluator {
  private static final List<JobProfile> JOBS = Arrays.asList(
    Records.newRecord(JobProfile.class),
    Records.newRecord(JobProfile.class),
    Records.newRecord(JobProfile.class),
    Records.newRecord(JobProfile.class)
  );

  private static final List<TaskProfile> TASKS0 = Arrays.asList(
    Records.newRecord(TaskProfile.class),
    Records.newRecord(TaskProfile.class)
  );

  private static final List<TaskProfile> TASKS2 = Arrays.asList(
    Records.newRecord(TaskProfile.class),
    Records.newRecord(TaskProfile.class)
  );

  private static final List<JobConfProxy> NOT_EXPIRED_CONFS = Arrays.asList(
    Records.newRecord(JobConfProxy.class),
    Records.newRecord(JobConfProxy.class),
    Records.newRecord(JobConfProxy.class),
    Records.newRecord(JobConfProxy.class)
  );

  private static final List<JobConfProxy> EXPIRED_CONFS = Arrays.asList(
    Records.newRecord(JobConfProxy.class),
    Records.newRecord(JobConfProxy.class),
    Records.newRecord(JobConfProxy.class),
    Records.newRecord(JobConfProxy.class)
  );

  static {
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

    for (int i = 0; i < 4; i++) {
      NOT_EXPIRED_CONFS.get(i).setId(Integer.toString(i));
      EXPIRED_CONFS.get(i).setId(Integer.toString(i));
    }

    NOT_EXPIRED_CONFS.get(1).setPropertyMap(Collections.singletonMap(APP_DEADLINE, "700"));
    NOT_EXPIRED_CONFS.get(3).setPropertyMap(Collections.singletonMap(APP_DEADLINE, "550"));

    EXPIRED_CONFS.get(1).setPropertyMap(Collections.singletonMap(APP_DEADLINE, "500"));
    EXPIRED_CONFS.get(3).setPropertyMap(Collections.singletonMap(APP_DEADLINE, "30"));

  }

  @Mock
  private Database db;

  @Before
  public void setUp() throws Exception {
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
    when(db.execute(FindByQueryCall.newInstance(DataEntityCollection.JOB_CONF_HISTORY, null)))
      .thenReturn(MultiEntityPayload.newInstance(DataEntityCollection.JOB_CONF_HISTORY, EXPIRED_CONFS));
    CompoundScorePayload result = new SimulationEvaluator(db).evaluate();
    assertThat(result, is(CompoundScorePayload.newInstance( 7.712307692307692, 79112.5, 0.0)));
  }


  @Test
  public void testEvaluationForNotExpired() {
    when(db.execute(FindByQueryCall.newInstance(DataEntityCollection.JOB_CONF_HISTORY, null)))
      .thenReturn(MultiEntityPayload.newInstance(DataEntityCollection.JOB_CONF_HISTORY, NOT_EXPIRED_CONFS));
    CompoundScorePayload result = new SimulationEvaluator(db).evaluate();
    assertThat(result, is(CompoundScorePayload.newInstance( 7.712307692307692, 0.0, 0.0)));
  }
}
