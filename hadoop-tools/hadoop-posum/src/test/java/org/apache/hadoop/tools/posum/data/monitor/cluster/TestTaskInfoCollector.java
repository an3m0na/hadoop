package org.apache.hadoop.tools.posum.data.monitor.cluster;

import org.apache.hadoop.mapreduce.v2.api.records.TaskState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.tools.posum.client.data.Database;
import org.apache.hadoop.tools.posum.common.records.call.FindByQueryCall;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.TaskProfile;
import org.apache.hadoop.tools.posum.common.records.payload.MultiEntityPayload;
import org.apache.hadoop.yarn.util.Records;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatcher;
import org.mockito.InjectMocks;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestTaskInfoCollector {
  @Mock
  private HadoopAPIClient apiMock;
  @Mock
  private Database dbMock;

  @InjectMocks
  private TaskInfoCollector testSubject = new TaskInfoCollector();

  private ClusterMonitorEntities entities;

  @Before
  public void init() {
    entities = new ClusterMonitorEntities();
  }

  @Test
  public void getFinishedTaskInfoTest() {
    when(dbMock.execute(Matchers.any(FindByQueryCall.class)))
      .thenReturn(MultiEntityPayload.newInstance(DataEntityCollection.TASK, Arrays.asList(entities.RUNNING_TASKS)));

    when(apiMock.getFinishedTasksInfo(entities.JOB_ID, Arrays.asList(entities.RUNNING_TASKS)))
      .thenReturn(Arrays.asList(entities.FINISHED_TASKS));
    when(apiMock.getFinishedTaskCounters(entities.JOB_ID, entities.FINISHED_TASKS[0].getId()))
      .thenReturn(entities.TASK_COUNTERS_MAP);
    when(apiMock.getFinishedTaskCounters(entities.JOB_ID, entities.FINISHED_TASKS[1].getId()))
      .thenReturn(entities.TASK_COUNTERS_REDUCE);

    TaskInfo info = testSubject.getFinishedTaskInfo(entities.FINISHED_JOB);

    verify(apiMock, times(1)).addFinishedAttemptInfo(entities.FINISHED_TASKS[0]);
    verify(apiMock, times(1)).addFinishedAttemptInfo(entities.FINISHED_TASKS[1]);
    assertThat(info.getTasks(), containsInAnyOrder(entities.FINISHED_TASKS));

    assertThat(info.getCounters(), containsInAnyOrder(entities.TASK_COUNTERS_MAP, entities.TASK_COUNTERS_REDUCE));
    assertThat(entities.FINISHED_TASKS[0].getInputBytes(), is(48L));
    assertThat(entities.FINISHED_TASKS[0].getOutputBytes(), is(2235L));
    assertThat(entities.FINISHED_TASKS[1].getInputBytes(), is(2235L));
    assertThat(entities.FINISHED_TASKS[1].getOutputBytes(), is(0L));
  }

  @Test
  public void getRunningTaskInfoTest() {
    when(dbMock.execute(Matchers.any(FindByQueryCall.class)))
      .thenReturn(MultiEntityPayload.newInstance(DataEntityCollection.TASK, Arrays.asList(entities.RUNNING_TASKS)));

    when(apiMock.getRunningTasksInfo(entities.RUNNING_JOB, Arrays.asList(entities.RUNNING_TASKS)))
      .thenReturn(Arrays.asList(entities.RUNNING_TASKS));
    when(apiMock.getRunningTaskCounters(entities.APP_ID, entities.JOB_ID, entities.RUNNING_TASKS[0].getId()))
      .thenReturn(entities.TASK_COUNTERS_MAP);
    when(apiMock.getRunningTaskCounters(entities.APP_ID, entities.JOB_ID, entities.RUNNING_TASKS[1].getId()))
      .thenReturn(entities.TASK_COUNTERS_REDUCE);
    when(apiMock.addRunningAttemptInfo(entities.RUNNING_TASKS[0])).thenReturn(true);
    when(apiMock.addRunningAttemptInfo(entities.RUNNING_TASKS[1])).thenReturn(true);

    TaskInfo info = testSubject.getRunningTaskInfo(entities.RUNNING_APP, entities.RUNNING_JOB);

    verify(apiMock, times(1)).addRunningAttemptInfo(entities.RUNNING_TASKS[0]);
    verify(apiMock, times(1)).addRunningAttemptInfo(entities.RUNNING_TASKS[1]);
    assertThat(info.getTasks(), containsInAnyOrder(entities.RUNNING_TASKS));

    assertThat(info.getCounters(), containsInAnyOrder(entities.TASK_COUNTERS_MAP, entities.TASK_COUNTERS_REDUCE));
    assertThat(entities.RUNNING_TASKS[0].getInputBytes(), is(48L));
    assertThat(entities.RUNNING_TASKS[0].getOutputBytes(), is(2235L));
    assertThat(entities.RUNNING_TASKS[1].getInputBytes(), is(2235L));
    assertThat(entities.RUNNING_TASKS[1].getOutputBytes(), is(0L));
  }

  @Test
  public void getSubmittedTaskInfoTest() {
    when(dbMock.execute(Matchers.any(FindByQueryCall.class)))
      .thenReturn(MultiEntityPayload.newInstance(DataEntityCollection.TASK, new ArrayList<TaskProfile>()));

    final TaskProfile mapStub = Records.newRecord(TaskProfile.class);
    mapStub.setId(entities.RUNNING_MAP_TASK.getId());
    mapStub.setType(TaskType.MAP);
    mapStub.setJobId(entities.RUNNING_JOB.getId());
    mapStub.setAppId(entities.RUNNING_JOB.getAppId());
    mapStub.setState(TaskState.NEW);
    mapStub.setSplitSize(entities.SPLIT_SIZES.get(0));
    mapStub.setSplitLocations(entities.SPLIT_LOCATIONS.get(0));

    final TaskProfile reduceStub = Records.newRecord(TaskProfile.class);
    reduceStub.setId(entities.RUNNING_REDUCE_TASK.getId());
    reduceStub.setType(TaskType.REDUCE);
    reduceStub.setState(TaskState.NEW);
    reduceStub.setJobId(entities.RUNNING_JOB.getId());
    reduceStub.setAppId(entities.RUNNING_JOB.getAppId());

    when(apiMock.getRunningTasksInfo(eq(entities.RUNNING_JOB), argThat(new ArgumentMatcher<List<TaskProfile>>() {
      @Override
      public boolean matches(Object o) {
        List<TaskProfile> createdStubs = (List<TaskProfile>) o;
        createdStubs.get(0).setLastUpdated(mapStub.getLastUpdated());
        createdStubs.get(1).setLastUpdated(reduceStub.getLastUpdated());
        return createdStubs.get(0).equals(mapStub) && createdStubs.get(1).equals(reduceStub);
      }
    }))).thenReturn(Arrays.asList(entities.RUNNING_TASKS));

    when(apiMock.getRunningTaskCounters(entities.APP_ID, entities.JOB_ID, entities.RUNNING_TASKS[0].getId()))
      .thenReturn(entities.TASK_COUNTERS_MAP);
    when(apiMock.getRunningTaskCounters(entities.APP_ID, entities.JOB_ID, entities.RUNNING_TASKS[1].getId()))
      .thenReturn(entities.TASK_COUNTERS_REDUCE);
    when(apiMock.addRunningAttemptInfo(entities.RUNNING_TASKS[0])).thenReturn(true);
    when(apiMock.addRunningAttemptInfo(entities.RUNNING_TASKS[1])).thenReturn(true);

    TaskInfo info = testSubject.getRunningTaskInfo(entities.RUNNING_APP, entities.RUNNING_JOB);

    verify(apiMock, times(1)).addRunningAttemptInfo(entities.RUNNING_TASKS[0]);
    verify(apiMock, times(1)).addRunningAttemptInfo(entities.RUNNING_TASKS[1]);
    assertThat(info.getTasks(), containsInAnyOrder(entities.RUNNING_TASKS));

    assertThat(info.getCounters(), containsInAnyOrder(entities.TASK_COUNTERS_MAP, entities.TASK_COUNTERS_REDUCE));
    assertThat(entities.RUNNING_TASKS[0].getInputBytes(), is(48L));
    assertThat(entities.RUNNING_TASKS[0].getOutputBytes(), is(2235L));
    assertThat(entities.RUNNING_TASKS[1].getInputBytes(), is(2235L));
    assertThat(entities.RUNNING_TASKS[1].getOutputBytes(), is(0L));
  }

}
