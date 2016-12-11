package org.apache.hadoop.tools.posum.data.monitor.cluster;

import org.apache.hadoop.tools.posum.client.data.Database;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
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

public class TestTaskInfoCollector {
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
        when(apiMock.getFinishedTaskCounters(entities.JOB_ID, entities.FINISHED_TASKS[0].getId()))
                .thenReturn(entities.TASK_COUNTERS);
        when(apiMock.getFinishedTaskCounters(entities.JOB_ID, entities.FINISHED_TASKS[1].getId()))
                .thenReturn(entities.TASK_COUNTERS);

        TaskInfo info = testSubject.getFinishedTaskInfo(entities.FINISHED_JOB);

        verify(apiMock, times(1)).addFinishedAttemptInfo(entities.FINISHED_TASKS[0]);
        verify(apiMock, times(1)).addFinishedAttemptInfo(entities.FINISHED_TASKS[1]);
        assertThat(info.getTasks(), containsInAnyOrder(entities.FINISHED_TASKS));

        assertThat(info.getCounters(), containsInAnyOrder(entities.TASK_COUNTERS, entities.TASK_COUNTERS));
        assertThat(entities.FINISHED_TASKS[0].getInputBytes(), is(48L));
        assertThat(entities.FINISHED_TASKS[0].getOutputBytes(), is(2235L));
        assertThat(entities.FINISHED_TASKS[1].getInputBytes(), is(2235L));
        assertThat(entities.FINISHED_TASKS[1].getOutputBytes(), is(0L));
    }

    @Test
    public void getRunningTaskInfoTest() {
        when(apiMock.getRunningTasksInfo(entities.RUNNING_JOB)).thenReturn(Arrays.asList(entities.RUNNING_TASKS));
        when(apiMock.getRunningTaskCounters(entities.APP_ID, entities.JOB_ID, entities.RUNNING_TASKS[0].getId()))
                .thenReturn(entities.TASK_COUNTERS);
        when(apiMock.getRunningTaskCounters(entities.APP_ID, entities.JOB_ID, entities.RUNNING_TASKS[1].getId()))
                .thenReturn(entities.TASK_COUNTERS);
        when(apiMock.addRunningAttemptInfo(entities.RUNNING_TASKS[0])).thenReturn(true);
        when(apiMock.addRunningAttemptInfo(entities.RUNNING_TASKS[1])).thenReturn(true);

        TaskInfo info = testSubject.getRunningTaskInfo(entities.RUNNING_JOB);

        verify(apiMock, times(1)).addRunningAttemptInfo(entities.RUNNING_TASKS[0]);
        verify(apiMock, times(1)).addRunningAttemptInfo(entities.RUNNING_TASKS[1]);
        assertThat(info.getTasks(), containsInAnyOrder(entities.RUNNING_TASKS));

        assertThat(info.getCounters(), containsInAnyOrder(entities.TASK_COUNTERS, entities.TASK_COUNTERS));
        assertThat(entities.RUNNING_TASKS[0].getInputBytes(), is(48L));
        assertThat(entities.RUNNING_TASKS[0].getOutputBytes(), is(2235L));
        assertThat(entities.RUNNING_TASKS[1].getInputBytes(), is(2235L));
        assertThat(entities.RUNNING_TASKS[1].getOutputBytes(), is(0L));
    }

} 
