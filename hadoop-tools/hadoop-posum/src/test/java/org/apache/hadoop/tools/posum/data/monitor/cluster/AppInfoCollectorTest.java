package org.apache.hadoop.tools.posum.data.monitor.cluster;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.client.data.Database;
import org.apache.hadoop.tools.posum.common.records.call.FindByQueryCall;
import org.apache.hadoop.tools.posum.common.records.call.StoreAllCall;
import org.apache.hadoop.tools.posum.common.records.call.StoreCall;
import org.apache.hadoop.tools.posum.common.records.call.TransactionCall;
import org.apache.hadoop.tools.posum.common.records.dataentity.AppProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.CountersProxy;
import org.apache.hadoop.tools.posum.common.records.dataentity.DatabaseReference;
import org.apache.hadoop.tools.posum.common.records.dataentity.GeneralDataEntity;
import org.apache.hadoop.tools.posum.common.records.dataentity.HistoryProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobConfProxy;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.TaskProfile;
import org.apache.hadoop.tools.posum.common.util.PosumConfiguration;
import org.apache.hadoop.tools.posum.data.mock.data.MockDataStoreImpl;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.APP;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.APP_HISTORY;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.COUNTER;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.COUNTER_HISTORY;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.HISTORY;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.JOB;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.JOB_CONF;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.JOB_CONF_HISTORY;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.JOB_HISTORY;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.TASK;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.TASK_HISTORY;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class AppInfoCollectorTest {

    @Mock
    private HadoopAPIClient apiMock;
    @Mock
    private JobInfoCollector jobInfoCollector;
    @Mock
    private TaskInfoCollector taskInfoCollector;

    @InjectMocks
    private AppInfoCollector testSubject;

    private Database dbMock;
    private ClusterMonitorEntities entities;
    private GeneralDataEntity[] expectedHistoryEntities;

    @Before
    public void init() {
        Configuration confMock = mock(Configuration.class);
        when(confMock.getBoolean(PosumConfiguration.MONITOR_KEEP_HISTORY, PosumConfiguration.MONITOR_KEEP_HISTORY_DEFAULT))
                .thenReturn(true);
        dbMock = Database.extractFrom(new MockDataStoreImpl(), DatabaseReference.getMain());
        testSubject = new AppInfoCollector(confMock, dbMock);
        MockitoAnnotations.initMocks(this);
        entities = new ClusterMonitorEntities();
        expectedHistoryEntities = new GeneralDataEntity[]{
                entities.RUNNING_APP,
                entities.RUNNING_JOB,
                entities.JOB_COUNTERS,
                entities.JOB_CONF,
                entities.RUNNING_TASKS[0],
                entities.RUNNING_TASKS[1],
                entities.TASK_COUNTERS,
                entities.TASK_COUNTERS
        };
    }

    @Test
    public void refreshRunningTest() {
        when(apiMock.getAppsInfo()).thenReturn(Arrays.asList(entities.RUNNING_APPS));
        when(jobInfoCollector.getRunningJobInfo(entities.RUNNING_APP)).thenReturn(new JobInfo(entities.RUNNING_JOB, entities.JOB_CONF, entities.JOB_COUNTERS));
        when(taskInfoCollector.getRunningTaskInfo(entities.RUNNING_JOB)).thenReturn(Arrays.asList(entities.RUNNING_TASKS));
        when(taskInfoCollector.updateRunningTasksFromCounters(Arrays.asList(entities.RUNNING_TASKS))).thenReturn(Arrays.asList(entities.TASK_COUNTERS, entities.TASK_COUNTERS));

        // first refresh on running job
        testSubject.refresh();

        List<AppProfile> apps = dbMock.executeDatabaseCall(FindByQueryCall.newInstance(APP, null)).getEntities();
        assertThat(apps, containsInAnyOrder(entities.RUNNING_APPS));
        List<JobProfile> jobs = dbMock.executeDatabaseCall(FindByQueryCall.newInstance(JOB, null)).getEntities();
        assertThat(jobs, containsInAnyOrder(entities.RUNNING_JOBS));
        List<JobConfProxy> confs = dbMock.executeDatabaseCall(FindByQueryCall.newInstance(JOB_CONF, null)).getEntities();
        assertThat(confs, containsInAnyOrder(entities.JOB_CONF));
        List<TaskProfile> tasks = dbMock.executeDatabaseCall(FindByQueryCall.newInstance(TASK, null)).getEntities();
        assertThat(tasks, containsInAnyOrder(entities.RUNNING_TASKS));
        List<CountersProxy> counters = dbMock.executeDatabaseCall(FindByQueryCall.newInstance(COUNTER, null)).getEntities();
        assertThat(counters, containsInAnyOrder(entities.JOB_COUNTERS, entities.TASK_COUNTERS));

        List<HistoryProfile> historyRecords = dbMock.executeDatabaseCall(FindByQueryCall.newInstance(HISTORY, null)).getEntities();
        List<GeneralDataEntity> historyEntities = new ArrayList<>(historyRecords.size());
        for (HistoryProfile historyRecord : historyRecords) {
            historyEntities.add(historyRecord.getOriginal());
        }
        assertThat(historyEntities, containsInAnyOrder(expectedHistoryEntities));

        // second refresh on running job
        testSubject.refresh();

        apps = dbMock.executeDatabaseCall(FindByQueryCall.newInstance(APP, null)).getEntities();
        assertThat(apps, containsInAnyOrder(entities.RUNNING_APPS));
        jobs = dbMock.executeDatabaseCall(FindByQueryCall.newInstance(JOB, null)).getEntities();
        assertThat(jobs, containsInAnyOrder(entities.RUNNING_JOBS));
        confs = dbMock.executeDatabaseCall(FindByQueryCall.newInstance(JOB_CONF, null)).getEntities();
        assertThat(confs, containsInAnyOrder(entities.JOB_CONF));
        tasks = dbMock.executeDatabaseCall(FindByQueryCall.newInstance(TASK, null)).getEntities();
        assertThat(tasks, containsInAnyOrder(entities.RUNNING_TASKS));
        counters = dbMock.executeDatabaseCall(FindByQueryCall.newInstance(COUNTER, null)).getEntities();
        assertThat(counters, containsInAnyOrder(entities.JOB_COUNTERS, entities.TASK_COUNTERS));

        historyRecords = dbMock.executeDatabaseCall(FindByQueryCall.newInstance(HISTORY, null)).getEntities();
        historyEntities = new ArrayList<>(historyRecords.size());
        for (HistoryProfile historyRecord : historyRecords) {
            historyEntities.add(historyRecord.getOriginal());
        }
        List<GeneralDataEntity> newExpectedHistoryEntities = new ArrayList<>(expectedHistoryEntities.length * 2);
        newExpectedHistoryEntities.addAll(Arrays.asList(expectedHistoryEntities));
        newExpectedHistoryEntities.addAll(Arrays.asList(expectedHistoryEntities));
        assertThat(historyEntities, containsInAnyOrder(newExpectedHistoryEntities.toArray()));
    }

    @Test
    public void refreshKnownFinishedTest() {
        storeRunningInfo();
        refreshRunningTest();
    }

    private void storeRunningInfo() {
        TransactionCall transaction = TransactionCall.newInstance()
                .addCall(StoreCall.newInstance(APP, entities.RUNNING_APP))
                .addCall(StoreCall.newInstance(JOB, entities.RUNNING_JOB))
                .addCall(StoreAllCall.newInstance(TASK, Arrays.asList(entities.RUNNING_TASKS)))
                .addCall(StoreCall.newInstance(JOB_CONF, entities.JOB_CONF))
                .addCall(StoreCall.newInstance(COUNTER, entities.JOB_COUNTERS))
                .addCall(StoreCall.newInstance(COUNTER, entities.TASK_COUNTERS));
        dbMock.executeDatabaseCall(transaction);
    }

    @Test
    public void refreshUnknownFinishedTest() {
        when(apiMock.getAppsInfo()).thenReturn(Arrays.asList(entities.FINISHED_APPS));
        when(jobInfoCollector.getFinishedJobInfo(entities.FINISHED_APP)).thenReturn(new JobInfo(entities.FINISHED_JOB, entities.JOB_CONF, entities.JOB_COUNTERS));
        when(taskInfoCollector.getFinishedTaskInfo(entities.FINISHED_JOB)).thenReturn(Arrays.asList(entities.FINISHED_TASKS));
        when(taskInfoCollector.updateFinishedTasksFromCounters(Arrays.asList(entities.FINISHED_TASKS))).thenReturn(Arrays.asList(entities.TASK_COUNTERS, entities.TASK_COUNTERS));

        testSubject.refresh();

        List<AppProfile> apps = dbMock.executeDatabaseCall(FindByQueryCall.newInstance(APP, null)).getEntities();
        assertThat(apps, empty());
        List<JobProfile> jobs = dbMock.executeDatabaseCall(FindByQueryCall.newInstance(JOB, null)).getEntities();
        assertThat(jobs, empty());
        List<JobConfProxy> confs = dbMock.executeDatabaseCall(FindByQueryCall.newInstance(JOB_CONF, null)).getEntities();
        assertThat(confs, empty());
        List<TaskProfile> tasks = dbMock.executeDatabaseCall(FindByQueryCall.newInstance(TASK, null)).getEntities();
        assertThat(tasks, empty());
        List<CountersProxy> counters = dbMock.executeDatabaseCall(FindByQueryCall.newInstance(COUNTER, null)).getEntities();
        assertThat(counters, empty());

        apps = dbMock.executeDatabaseCall(FindByQueryCall.newInstance(APP_HISTORY, null)).getEntities();
        assertThat(apps, containsInAnyOrder(entities.FINISHED_APPS));
        jobs = dbMock.executeDatabaseCall(FindByQueryCall.newInstance(JOB_HISTORY, null)).getEntities();
        assertThat(jobs, containsInAnyOrder(entities.FINISHED_JOB));
        confs = dbMock.executeDatabaseCall(FindByQueryCall.newInstance(JOB_CONF_HISTORY, null)).getEntities();
        assertThat(confs, containsInAnyOrder(entities.JOB_CONF));
        tasks = dbMock.executeDatabaseCall(FindByQueryCall.newInstance(TASK_HISTORY, null)).getEntities();
        assertThat(tasks, containsInAnyOrder(entities.FINISHED_TASKS));
        counters = dbMock.executeDatabaseCall(FindByQueryCall.newInstance(COUNTER_HISTORY, null)).getEntities();
        assertThat(counters, containsInAnyOrder(entities.TASK_COUNTERS, entities.JOB_COUNTERS));

        List<HistoryProfile> historyRecords = dbMock.executeDatabaseCall(FindByQueryCall.newInstance(HISTORY, null)).getEntities();
        assertThat(historyRecords, empty());
    }
} 
