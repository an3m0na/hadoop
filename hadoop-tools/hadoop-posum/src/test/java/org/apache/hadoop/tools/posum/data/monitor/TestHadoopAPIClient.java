package org.apache.hadoop.tools.posum.data.monitor;

import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.tools.posum.common.records.dataentity.AppProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.CountersProxy;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobConfProxy;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.TaskProfile;
import org.apache.hadoop.tools.posum.common.records.payload.CounterGroupInfoPayload;
import org.apache.hadoop.tools.posum.common.records.payload.CounterInfoPayload;
import org.apache.hadoop.tools.posum.common.util.RestClient;
import org.apache.hadoop.tools.posum.common.util.RestClient.TrackingUI;
import org.apache.hadoop.tools.posum.test.Utils;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.util.Records;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isIn;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

public class TestHadoopAPIClient {

    @Mock
    private RestClient restClient;
    @Mock
    private JobProfile jobMock;
    @Mock
    private JobProfile previousJobMock;
    @Mock
    private AppProfile appMock;
    @Mock
    private TaskProfile taskMock;

    @InjectMocks
    private HadoopAPIClient testSubject;

    private AppProfile FINISHED_APP, RUNNING_APP;
    private String APP_ID, JOB_ID;
    private AppProfile[] APPS;
    private JobProfile FINISHED_JOB, RUNNING_JOB;
    private TaskProfile[] FINISHED_TASKS, RUNNING_TASKS;
    private TaskProfile FINISHED_REDUCE_TASK, FINISHED_MAP_TASK, RUNNING_MAP_TASK, RUNNING_REDUCE_TASK, DETAILED_REDUCE_TASK, FINISHED_DETAILED_REDUCE_TASK;
    private CountersProxy JOB_COUNTERS, TASK_COUNTERS;
    private JobConfProxy JOB_CONF;

    @Before
    public void init() throws Exception {
        testSubject = new HadoopAPIClient();
        MockitoAnnotations.initMocks(this);

        APP_ID = "application_1326821518301_0005";

        RUNNING_APP = Records.newRecord(AppProfile.class);
        RUNNING_APP.setId(APP_ID);
        RUNNING_APP.setQueue("a1");
        RUNNING_APP.setUser("user1");
        RUNNING_APP.setState(YarnApplicationState.RUNNING);
        RUNNING_APP.setName("Sleep job");
        RUNNING_APP.setStartTime(1326824544552L);
        RUNNING_APP.setTrackingUI(TrackingUI.AM);
        RUNNING_APP.setStatus(FinalApplicationStatus.UNDEFINED);

        FINISHED_APP = RUNNING_APP.copy();
        FINISHED_APP.setFinishTime(1326824991300L);
        FINISHED_APP.setState(YarnApplicationState.FINISHED);
        FINISHED_APP.setTrackingUI(TrackingUI.HISTORY);
        FINISHED_APP.setStatus(FinalApplicationStatus.SUCCEEDED);

        APPS = new AppProfile[]{FINISHED_APP};

        JOB_ID = "job_1326821518301_0005";

        RUNNING_JOB = Records.newRecord(JobProfile.class);
        RUNNING_JOB.setId(JOB_ID);
        RUNNING_JOB.setAppId(APP_ID);
        RUNNING_JOB.setFinishTime(0L);
        RUNNING_JOB.setQueue("a1");
        RUNNING_JOB.setUser("user1");
        RUNNING_JOB.setName("Sleep job");
        RUNNING_JOB.setStartTime(1326381446529L);
        RUNNING_JOB.setUberized(false);
        RUNNING_JOB.setTotalMapTasks(1);
        RUNNING_JOB.setTotalReduceTasks(1);
        RUNNING_JOB.setState(JobState.RUNNING);
        RUNNING_JOB.setCompletedMaps(0);
        RUNNING_JOB.setCompletedReduces(0);
        RUNNING_JOB.setMapProgress(58f);
        RUNNING_JOB.setReduceProgress(0f);

        FINISHED_JOB = RUNNING_JOB.copy();
        FINISHED_JOB.setSubmitTime(1326381446500L);
        FINISHED_JOB.setFinishTime(1326381582106L);
        FINISHED_JOB.setState(JobState.SUCCEEDED);
        FINISHED_JOB.setCompletedMaps(1);
        FINISHED_JOB.setCompletedReduces(1);
        FINISHED_JOB.setAvgMapDuration(2638L);
        FINISHED_JOB.setAvgReduceDuration(130090L);
        FINISHED_JOB.setAvgShuffleTime(2540L);
        FINISHED_JOB.setAvgMergeTime(2589L);
        FINISHED_JOB.setAvgReduceTime(124961L);
        FINISHED_JOB.setMapProgress(100f);
        FINISHED_JOB.setReduceProgress(100f);

        RUNNING_MAP_TASK = Records.newRecord(TaskProfile.class);
        RUNNING_MAP_TASK.setId("task_1326821518301_0005_m_0");
        RUNNING_MAP_TASK.setAppId(APP_ID);
        RUNNING_MAP_TASK.setJobId(JOB_ID);
        RUNNING_MAP_TASK.setType(TaskType.MAP);
        RUNNING_MAP_TASK.setStartTime(1326381446541L);
        RUNNING_MAP_TASK.setFinishTime(0L);
        RUNNING_MAP_TASK.setState(TaskState.RUNNING);
        RUNNING_MAP_TASK.setReportedProgress(58f);

        RUNNING_REDUCE_TASK = Records.newRecord(TaskProfile.class);
        RUNNING_REDUCE_TASK.setId("task_1326821518301_0005_r_0");
        RUNNING_REDUCE_TASK.setAppId(APP_ID);
        RUNNING_REDUCE_TASK.setJobId(JOB_ID);
        RUNNING_REDUCE_TASK.setType(TaskType.REDUCE);
        RUNNING_REDUCE_TASK.setStartTime(1326381446544L);
        RUNNING_REDUCE_TASK.setFinishTime(0L);
        RUNNING_REDUCE_TASK.setState(TaskState.RUNNING);
        RUNNING_REDUCE_TASK.setReportedProgress(0f);

        RUNNING_TASKS = new TaskProfile[]{RUNNING_MAP_TASK, RUNNING_REDUCE_TASK};

        FINISHED_MAP_TASK = RUNNING_MAP_TASK.copy();
        FINISHED_MAP_TASK.setAppId(null);
        FINISHED_MAP_TASK.setFinishTime(1326381453318L);
        FINISHED_MAP_TASK.setSuccessfulAttempt("attempt_1326821518301_0005_m_0_0");
        FINISHED_MAP_TASK.setState(TaskState.SUCCEEDED);
        FINISHED_MAP_TASK.setReportedProgress(100f);

        FINISHED_REDUCE_TASK = RUNNING_REDUCE_TASK.copy();
        FINISHED_REDUCE_TASK.setAppId(null);
        FINISHED_REDUCE_TASK.setFinishTime(1326381582103L);
        FINISHED_REDUCE_TASK.setSuccessfulAttempt("attempt_1326821518301_0005_r_0_0");
        FINISHED_REDUCE_TASK.setState(TaskState.SUCCEEDED);
        FINISHED_REDUCE_TASK.setReportedProgress(100f);

        FINISHED_TASKS = new TaskProfile[]{FINISHED_MAP_TASK, FINISHED_REDUCE_TASK};

        DETAILED_REDUCE_TASK = RUNNING_REDUCE_TASK.copy();
        DETAILED_REDUCE_TASK.setShuffleTime(2592L);
        DETAILED_REDUCE_TASK.setReduceTime(0L);
        DETAILED_REDUCE_TASK.setMergeTime(47L);
        DETAILED_REDUCE_TASK.setHttpAddress("host.domain.com");

        FINISHED_DETAILED_REDUCE_TASK = DETAILED_REDUCE_TASK.copy();
        FINISHED_DETAILED_REDUCE_TASK.setReduceTime(311L);


        JOB_COUNTERS = Records.newRecord(CountersProxy.class);
        JOB_COUNTERS.setId(JOB_ID);
        JOB_COUNTERS.setCounterGroup(createCounterGroups(true));

        TASK_COUNTERS = Records.newRecord(CountersProxy.class);
        TASK_COUNTERS.setId(RUNNING_MAP_TASK.getId());
        TASK_COUNTERS.setTaskCounterGroup(createCounterGroups(false));

        JOB_CONF = Records.newRecord(JobConfProxy.class);
        JOB_CONF.setId(JOB_ID);
        JOB_CONF.setConfPath("hdfs://host.domain.com:9000/user/user1/.staging/job_1326381300833_0002/job.xml");
        Map<String, String> properties = new HashMap<>();
        properties.put("dfs.datanode.data.dir", "/home/hadoop/hdfs/data");
        properties.put("hadoop.http.filter.initializers", "org.apache.hadoop.yarn.server.webproxy.amfilter.AmFilterInitializer");
        properties.put("mapreduce.cluster.temp.dir", "/home/hadoop/tmp");
        JOB_CONF.setPropertyMap(properties);
    }

    private List<CounterGroupInfoPayload> createCounterGroups(boolean includeMapAndReduceCounters) {
        List<CounterGroupInfoPayload> counterGroups = Arrays.asList(
                CounterGroupInfoPayload.newInstance("Shuffle Errors", Arrays.asList(
                        CounterInfoPayload.newInstance("BAD_ID", 0),
                        CounterInfoPayload.newInstance("CONNECTION", 0),
                        CounterInfoPayload.newInstance("IO_ERROR", 0),
                        CounterInfoPayload.newInstance("WRONG_LENGTH", 0),
                        CounterInfoPayload.newInstance("WRONG_MAP", 0),
                        CounterInfoPayload.newInstance("WRONG_REDUCE", 0)
                )),
                CounterGroupInfoPayload.newInstance("org.apache.hadoop.mapreduce.FileSystemCounter", Arrays.asList(
                        CounterInfoPayload.newInstance("FILE_BYTES_READ", 2483),
                        CounterInfoPayload.newInstance("FILE_BYTES_WRITTEN", 108525),
                        CounterInfoPayload.newInstance("FILE_READ_OPS", 0),
                        CounterInfoPayload.newInstance("FILE_LARGE_READ_OPS", 0),
                        CounterInfoPayload.newInstance("FILE_WRITE_OPS", 0),
                        CounterInfoPayload.newInstance("HDFS_BYTES_READ", 48),
                        CounterInfoPayload.newInstance("HDFS_BYTES_WRITTEN", 0),
                        CounterInfoPayload.newInstance("HDFS_READ_OPS", 1),
                        CounterInfoPayload.newInstance("HDFS_LARGE_READ_OPS", 0),
                        CounterInfoPayload.newInstance("HDFS_WRITE_OPS", 0)
                )),
                CounterGroupInfoPayload.newInstance("org.apache.hadoop.mapreduce.TaskCounter", Arrays.asList(
                        CounterInfoPayload.newInstance("MAP_INPUT_RECORDS", 1),
                        CounterInfoPayload.newInstance("MAP_OUTPUT_RECORDS", 1200),
                        CounterInfoPayload.newInstance("MAP_OUTPUT_BYTES", 4800),
                        CounterInfoPayload.newInstance("MAP_OUTPUT_MATERIALIZED_BYTES", 2235),
                        CounterInfoPayload.newInstance("SPLIT_RAW_BYTES", 48),
                        CounterInfoPayload.newInstance("COMBINE_INPUT_RECORDS", 0),
                        CounterInfoPayload.newInstance("COMBINE_OUTPUT_RECORDS", 0),
                        CounterInfoPayload.newInstance("REDUCE_INPUT_GROUPS", 1200),
                        CounterInfoPayload.newInstance("REDUCE_SHUFFLE_BYTES", 2235),
                        CounterInfoPayload.newInstance("REDUCE_INPUT_RECORDS", 1200),
                        CounterInfoPayload.newInstance("REDUCE_OUTPUT_RECORDS", 0),
                        CounterInfoPayload.newInstance("SPILLED_RECORDS", 2400),
                        CounterInfoPayload.newInstance("SHUFFLED_MAPS", 1),
                        CounterInfoPayload.newInstance("FAILED_SHUFFLE", 0),
                        CounterInfoPayload.newInstance("MERGED_MAP_OUTPUTS", 1),
                        CounterInfoPayload.newInstance("GC_TIME_MILLIS", 113),
                        CounterInfoPayload.newInstance("CPU_MILLISECONDS", 1830),
                        CounterInfoPayload.newInstance("PHYSICAL_MEMORY_BYTES", 478068736),
                        CounterInfoPayload.newInstance("VIRTUAL_MEMORY_BYTES", 2159284224L),
                        CounterInfoPayload.newInstance("COMMITTED_HEAP_BYTES", 378863616)
                )),
                CounterGroupInfoPayload.newInstance("org.apache.hadoop.mapreduce.lib.input.FileInputFormatCounter", Arrays.asList(
                        CounterInfoPayload.newInstance("BYTES_READ", 0)
                )),
                CounterGroupInfoPayload.newInstance("org.apache.hadoop.mapreduce.lib.output.FileOutputFormatCounter", Arrays.asList(
                        CounterInfoPayload.newInstance("BYTES_WRITTEN", 0)
                ))
        );
        if (includeMapAndReduceCounters) {
            for (CounterGroupInfoPayload counterGroup : counterGroups) {
                for (CounterInfoPayload counterInfoPayload : counterGroup.getCounter()) {
                    counterInfoPayload.setMapCounterValue(0);
                    counterInfoPayload.setReduceCounterValue(0);
                }
            }
        }
        return counterGroups;
    }

    @Test
    public void getAppsInfoTest() throws Exception {
        when(restClient.getInfo(eq(String.class), eq(TrackingUI.RM), eq("cluster/apps"), any(String[].class)))
                .thenReturn("{}");
        List<AppProfile> ret = testSubject.getAppsInfo();
        assertThat(ret, empty());

        when(restClient.getInfo(eq(String.class), eq(TrackingUI.RM), eq("cluster/apps"), any(String[].class)))
                .thenReturn(Utils.getApiJson("apps.json"));
        ret = testSubject.getAppsInfo();
        assertThat(ret, containsInAnyOrder(APPS));
    }

    @Test
    public void checkAppFinishedTest() throws Exception {
        String finishedJson = Utils.getApiJson("apps_app.json");
        String unfinishedJson = finishedJson.replace("History", "ApplicationMaster");

        when(restClient.getInfo(eq(String.class), eq(TrackingUI.RM), eq("cluster/apps/%s"), any(String[].class)))
                .thenReturn(unfinishedJson);
        AppProfile savedApp = RUNNING_APP;
        assertFalse(testSubject.checkAppFinished(RUNNING_APP));
        assertThat(RUNNING_APP, is(savedApp));

        when(restClient.getInfo(eq(String.class), eq(TrackingUI.RM), eq("cluster/apps/%s"), any(String[].class)))
                .thenReturn(finishedJson);
        assertTrue(testSubject.checkAppFinished(RUNNING_APP));
        assertThat(RUNNING_APP, is(FINISHED_APP));
    }

    @Test
    public void getFinishedJobInfoTest() throws Exception {
        when(restClient.getInfo(eq(String.class), eq(TrackingUI.HISTORY), eq("jobs"), any(String[].class)))
                .thenReturn("{}");
        JobProfile ret = testSubject.getFinishedJobInfo(APP_ID);
        assertThat(ret, nullValue());

        when(restClient.getInfo(eq(String.class), eq(TrackingUI.HISTORY), eq("jobs"), any(String[].class)))
                .thenReturn(Utils.getApiJson("history_jobs.json"));
        when(restClient.getInfo(eq(String.class), eq(TrackingUI.HISTORY), eq("jobs/%s"), any(String[].class)))
                .thenReturn(Utils.getApiJson("history_jobs_job.json"));
        ret = testSubject.getFinishedJobInfo(APP_ID, JOB_ID, RUNNING_JOB);
        assertThat(ret, is(FINISHED_JOB));
    }

    @Test
    public void getRunningJobInfoTest() throws Exception {
        when(restClient.getInfo(eq(String.class), eq(TrackingUI.AM), eq("jobs"), any(String[].class)))
                .thenReturn("{}");
        JobProfile ret = testSubject.getRunningJobInfo(APP_ID, RUNNING_APP.getQueue(), null);
        assertThat(ret, nullValue());

        when(restClient.getInfo(eq(String.class), eq(TrackingUI.AM), eq("jobs"), any(String[].class)))
                .thenReturn(Utils.getApiJson("jobs.json"));
        ret = testSubject.getRunningJobInfo(APP_ID, RUNNING_APP.getQueue(), null);
        assertThat(ret, is(RUNNING_JOB));
    }

    @Test
    public void getFinishedTasksInfoTest() throws Exception {
        when(restClient.getInfo(eq(String.class), eq(TrackingUI.HISTORY), eq("jobs/%s/tasks"), any(String[].class)))
                .thenReturn("{}");
        List<TaskProfile> ret = testSubject.getFinishedTasksInfo(JOB_ID);
        assertThat(ret, empty());

        when(restClient.getInfo(eq(String.class), eq(TrackingUI.HISTORY), eq("jobs/%s/tasks"), any(String[].class)))
                .thenReturn(Utils.getApiJson("history_tasks.json"));
        ret = testSubject.getFinishedTasksInfo(JOB_ID);
        assertThat(ret, containsInAnyOrder(FINISHED_TASKS));
    }

    @Test
    public void getRunningTasksInfoTest() throws Exception {
        when(restClient.getInfo(eq(String.class), eq(TrackingUI.AM), eq("jobs/%s/tasks"), any(String[].class)))
                .thenReturn("{}");
        List<TaskProfile> ret = testSubject.getRunningTasksInfo(RUNNING_JOB);
        assertThat(ret, empty());

        when(restClient.getInfo(eq(String.class), eq(TrackingUI.AM), eq("jobs/%s/tasks"), any(String[].class)))
                .thenReturn(Utils.getApiJson("tasks.json"));
        ret = testSubject.getRunningTasksInfo(RUNNING_JOB);
        assertThat(ret, containsInAnyOrder(RUNNING_TASKS));
    }

    @Test
    public void addRunningAttemptInfoTest() throws Exception {
        when(restClient.getInfo(eq(String.class), eq(TrackingUI.AM), eq("jobs/%s/tasks/%s/attempts"), any(String[].class)))
                .thenReturn("{}");
        TaskProfile savedTask = RUNNING_REDUCE_TASK.copy();
        boolean ret = testSubject.addRunningAttemptInfo(RUNNING_REDUCE_TASK);
        assertFalse(ret);
        assertThat(RUNNING_REDUCE_TASK, is(savedTask));

        when(restClient.getInfo(eq(String.class), eq(TrackingUI.AM), eq("jobs/%s/tasks/%s/attempts"), any(String[].class)))
                .thenReturn(Utils.getApiJson("task_attempts.json"));
        ret = testSubject.addRunningAttemptInfo(RUNNING_REDUCE_TASK);
        assertTrue(ret);
        assertThat(RUNNING_REDUCE_TASK, is(DETAILED_REDUCE_TASK));
    }

    @Test
    public void addFinishedAttemptInfoTest() throws Exception {
        when(restClient.getInfo(eq(String.class), eq(TrackingUI.HISTORY), eq("jobs/%s/tasks/%s/attempts"), any(String[].class)))
                .thenReturn("{}");
        TaskProfile savedTask = RUNNING_REDUCE_TASK.copy();
        testSubject.addFinishedAttemptInfo(RUNNING_REDUCE_TASK);
        assertThat(RUNNING_REDUCE_TASK, is(savedTask));

        when(restClient.getInfo(eq(String.class), eq(TrackingUI.HISTORY), eq("jobs/%s/tasks/%s/attempts"), any(String[].class)))
                .thenReturn(Utils.getApiJson("history_task_attempts.json"));
        testSubject.addFinishedAttemptInfo(RUNNING_REDUCE_TASK);
        assertThat(RUNNING_REDUCE_TASK, is(FINISHED_DETAILED_REDUCE_TASK));
    }

    @Test
    public void jobCountersMappingTest() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        HadoopAPIClient.JobCountersWrapper counters = mapper.readValue(Utils.getApiJson("job_counters.json"), HadoopAPIClient.JobCountersWrapper.class);
        assertThat(counters.jobCounters, is(JOB_COUNTERS));
    }

    @Test
    public void taskCountersMappingTest() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        HadoopAPIClient.TaskCountersWrapper counters = mapper.readValue(Utils.getApiJson("task_counters.json"), HadoopAPIClient.TaskCountersWrapper.class);
        assertThat(counters.jobTaskCounters, is(TASK_COUNTERS));
    }

    @Test
    public void getFinishedJobConfTest() throws Exception {

        when(restClient.getInfo(eq(String.class), eq(TrackingUI.HISTORY), eq("jobs/%s/conf"), any(String[].class)))
                .thenReturn("{}");
        JobConfProxy ret = testSubject.getFinishedJobConf(JOB_ID);
        assertThat(ret, nullValue());

        when(restClient.getInfo(eq(String.class), eq(TrackingUI.HISTORY), eq("jobs/%s/conf"), any(String[].class)))
                .thenReturn(Utils.getApiJson("job_conf.json"));
        ret = testSubject.getFinishedJobConf(JOB_ID);
        assertThat(ret.getId(), is(JOB_ID));
        assertThat(ret.getPropertyMap().entrySet(), everyItem(isIn(JOB_CONF.getPropertyMap().entrySet())));
    }

} 
