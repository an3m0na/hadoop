package org.apache.hadoop.tools.posum.data.monitor.cluster;

import org.apache.hadoop.tools.posum.client.data.Database;
import org.apache.hadoop.tools.posum.common.records.dataentity.CountersProxy;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.TaskProfile;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TaskInfoCollectorTest {
    private TaskInfoCollector testSubject;
    @Mock
    private List tasksMock;
    @Mock
    private Database dbMock;
    @Mock
    private JobProfile jobMock;
    @Mock
    private HadoopAPIClient apiMock;

    private ClusterMonitorEntities entities;


    @Before
    public void init() {
        MockitoAnnotations.initMocks(this);
        testSubject = new TaskInfoCollector(apiMock);
        entities = new ClusterMonitorEntities();
    }

    @Test
    public void getFinishedTaskInfoTest() {
        when(apiMock.getFinishedTasksInfo(entities.JOB_ID)).thenReturn(Arrays.asList(entities.FINISHED_TASKS));

        List<TaskProfile> ret = testSubject.getFinishedTaskInfo(entities.FINISHED_JOB);

        verify(apiMock, times(1)).addFinishedAttemptInfo(entities.FINISHED_TASKS[0]);
        verify(apiMock, times(1)).addFinishedAttemptInfo(entities.FINISHED_TASKS[1]);
        assertThat(ret, containsInAnyOrder(entities.FINISHED_TASKS));
    }

    @Test
    public void updateFinishedTasksFromCountersTest() {
        when(apiMock.getFinishedTaskCounters(entities.JOB_ID, entities.FINISHED_TASKS[0].getId()))
                .thenReturn(entities.TASK_COUNTERS);
        when(apiMock.getFinishedTaskCounters(entities.JOB_ID, entities.FINISHED_TASKS[1].getId()))
                .thenReturn(entities.TASK_COUNTERS);

        List<CountersProxy> ret = testSubject.updateFinishedTasksFromCounters(Arrays.asList(entities.FINISHED_TASKS));

        assertThat(ret, containsInAnyOrder(entities.TASK_COUNTERS, entities.TASK_COUNTERS));
        assertThat(entities.FINISHED_TASKS[0].getInputBytes(), is(48L));
        assertThat(entities.FINISHED_TASKS[0].getOutputBytes(), is(2235L));
        assertThat(entities.FINISHED_TASKS[1].getInputBytes(), is(2235L));
        assertThat(entities.FINISHED_TASKS[1].getOutputBytes(), is(0L));
    }

    @Test
    public void getRunningTaskInfoTest() {
        when(apiMock.getFinishedTasksInfo(entities.JOB_ID)).thenReturn(Arrays.asList(entities.RUNNING_TASKS));

        List<TaskProfile> ret = testSubject.getRunningTaskInfo(entities.RUNNING_JOB);

        verify(apiMock, times(1)).addRunningAttemptInfo(entities.RUNNING_TASKS[0]);
        verify(apiMock, times(1)).addRunningAttemptInfo(entities.RUNNING_TASKS[1]);
        assertThat(ret, containsInAnyOrder(entities.RUNNING_TASKS));
    }

    @Test
    public void updateRunningTasksFromCountersTest() {
        when(apiMock.getRunningTaskCounters(entities.APP_ID, entities.JOB_ID, entities.RUNNING_TASKS[0].getId()))
                .thenReturn(entities.TASK_COUNTERS);
        when(apiMock.getRunningTaskCounters(entities.APP_ID, entities.JOB_ID, entities.RUNNING_TASKS[1].getId()))
                .thenReturn(entities.TASK_COUNTERS);

        List<CountersProxy> ret = testSubject.updateRunningTasksFromCounters(Arrays.asList(entities.RUNNING_TASKS));

        assertThat(ret, containsInAnyOrder(entities.TASK_COUNTERS, entities.TASK_COUNTERS));
        assertThat(entities.RUNNING_TASKS[0].getInputBytes(), is(48L));
        assertThat(entities.RUNNING_TASKS[0].getOutputBytes(), is(2235L));
        assertThat(entities.RUNNING_TASKS[1].getInputBytes(), is(2235L));
        assertThat(entities.RUNNING_TASKS[1].getOutputBytes(), is(0L));
    }


} 
