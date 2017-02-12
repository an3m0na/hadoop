package org.apache.hadoop.tools.posum.data.monitor.cluster;

import org.apache.hadoop.tools.posum.common.records.dataentity.AppProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobConfProxy;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.TaskProfile;
import org.apache.hadoop.tools.posum.common.util.RestClient;
import org.apache.hadoop.tools.posum.common.util.RestClient.TrackingUI;
import org.apache.hadoop.tools.posum.test.Utils;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.List;

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

  private ClusterMonitorEntities entities;

  @Before
  public void init() throws Exception {
    testSubject = new HadoopAPIClient();
    MockitoAnnotations.initMocks(this);
    entities = new ClusterMonitorEntities();
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
    ret.get(0).setLastUpdated(entities.FINISHED_APP.getLastUpdated());
    assertThat(ret, containsInAnyOrder(entities.FINISHED_APPS));
  }

  @Test
  public void checkAppFinishedTest() throws Exception {
    String finishedJson = Utils.getApiJson("apps_app.json");
    String unfinishedJson = finishedJson.replace("History", "ApplicationMaster");

    when(restClient.getInfo(eq(String.class), eq(TrackingUI.RM), eq("cluster/apps/%s"), any(String[].class)))
      .thenReturn(unfinishedJson);
    AppProfile savedApp = entities.RUNNING_APP;
    assertFalse(testSubject.checkAppFinished(entities.RUNNING_APP));
    assertThat(entities.RUNNING_APP, is(savedApp));

    when(restClient.getInfo(eq(String.class), eq(TrackingUI.RM), eq("cluster/apps/%s"), any(String[].class)))
      .thenReturn(finishedJson);
    assertTrue(testSubject.checkAppFinished(entities.RUNNING_APP));
    assertThat(entities.RUNNING_APP, is(entities.FINISHED_APP));
  }

  @Test
  public void getFinishedJobInfoTest() throws Exception {
    when(restClient.getInfo(eq(String.class), eq(TrackingUI.HISTORY), eq("jobs"), any(String[].class)))
      .thenReturn("{}");
    JobProfile ret = testSubject.getFinishedJobInfo(entities.APP_ID);
    assertThat(ret, nullValue());

    when(restClient.getInfo(eq(String.class), eq(TrackingUI.HISTORY), eq("jobs"), any(String[].class)))
      .thenReturn(Utils.getApiJson("history_jobs.json"));
    when(restClient.getInfo(eq(String.class), eq(TrackingUI.HISTORY), eq("jobs/%s"), any(String[].class)))
      .thenReturn(Utils.getApiJson("history_jobs_job.json"));
    ret = testSubject.getFinishedJobInfo(entities.APP_ID, entities.JOB_ID, entities.RUNNING_JOB);
    assertThat(ret, is(entities.FINISHED_JOB));
  }

  @Test
  public void getRunningJobInfoTest() throws Exception {
    when(restClient.getInfo(eq(String.class), eq(TrackingUI.AM), eq("jobs"), any(String[].class)))
      .thenReturn("{}");
    JobProfile ret = testSubject.getRunningJobInfo(entities.APP_ID, entities.RUNNING_APP.getQueue(), null);
    assertThat(ret, nullValue());

    when(restClient.getInfo(eq(String.class), eq(TrackingUI.AM), eq("jobs"), any(String[].class)))
      .thenReturn(Utils.getApiJson("jobs.json"));
    ret = testSubject.getRunningJobInfo(entities.APP_ID, entities.RUNNING_APP.getQueue(), null);
    ret.setLastUpdated(entities.RUNNING_JOB.getLastUpdated());
    assertThat(ret, is(entities.RUNNING_JOB));
  }

  @Test
  public void getFinishedTasksInfoTest() throws Exception {
    when(restClient.getInfo(eq(String.class), eq(TrackingUI.HISTORY), eq("jobs/%s/tasks"), any(String[].class)))
      .thenReturn("{}");
    List<TaskProfile> ret = testSubject.getFinishedTasksInfo(entities.JOB_ID);
    assertThat(ret, empty());

    when(restClient.getInfo(eq(String.class), eq(TrackingUI.HISTORY), eq("jobs/%s/tasks"), any(String[].class)))
      .thenReturn(Utils.getApiJson("history_tasks.json"));
    ret = testSubject.getFinishedTasksInfo(entities.JOB_ID);
    ret.get(0).setLastUpdated(entities.FINISHED_TASKS[0].getLastUpdated());
    ret.get(1).setLastUpdated(entities.FINISHED_TASKS[1].getLastUpdated());
    assertThat(ret, containsInAnyOrder(entities.FINISHED_TASKS));
  }

  @Test
  public void getRunningTasksInfoTest() throws Exception {
    when(restClient.getInfo(eq(String.class), eq(TrackingUI.AM), eq("jobs/%s/tasks"), any(String[].class)))
      .thenReturn("{}");
    List<TaskProfile> ret = testSubject.getRunningTasksInfo(entities.RUNNING_JOB);
    assertThat(ret, empty());

    when(restClient.getInfo(eq(String.class), eq(TrackingUI.AM), eq("jobs/%s/tasks"), any(String[].class)))
      .thenReturn(Utils.getApiJson("tasks.json"));

    ret = testSubject.getRunningTasksInfo(entities.RUNNING_JOB);

    ret.get(0).setLastUpdated(entities.RUNNING_MAP_TASK.getLastUpdated());
    ret.get(1).setLastUpdated(entities.RUNNING_REDUCE_TASK.getLastUpdated());
    assertThat(ret, containsInAnyOrder(entities.RUNNING_TASKS));
  }

  @Test
  public void addRunningAttemptInfoTest() throws Exception {
    when(restClient.getInfo(eq(String.class), eq(TrackingUI.AM), eq("jobs/%s/tasks/%s/attempts"), any(String[].class)))
      .thenReturn("{}");
    TaskProfile savedTask = entities.RUNNING_REDUCE_TASK.copy();
    boolean ret = testSubject.addRunningAttemptInfo(entities.RUNNING_REDUCE_TASK);
    assertFalse(ret);
    assertThat(entities.RUNNING_REDUCE_TASK, is(savedTask));

    when(restClient.getInfo(eq(String.class), eq(TrackingUI.AM), eq("jobs/%s/tasks/%s/attempts"), any(String[].class)))
      .thenReturn(Utils.getApiJson("task_attempts.json"));
    ret = testSubject.addRunningAttemptInfo(entities.RUNNING_REDUCE_TASK);
    assertTrue(ret);
    assertThat(entities.RUNNING_REDUCE_TASK, is(entities.DETAILED_REDUCE_TASK));
  }

  @Test
  public void addFinishedAttemptInfoTest() throws Exception {
    when(restClient.getInfo(eq(String.class), eq(TrackingUI.HISTORY), eq("jobs/%s/tasks/%s/attempts"), any(String[].class)))
      .thenReturn("{}");
    TaskProfile savedTask = entities.RUNNING_REDUCE_TASK.copy();
    testSubject.addFinishedAttemptInfo(entities.RUNNING_REDUCE_TASK);
    assertThat(entities.RUNNING_REDUCE_TASK, is(savedTask));

    when(restClient.getInfo(eq(String.class), eq(TrackingUI.HISTORY), eq("jobs/%s/tasks/%s/attempts"), any(String[].class)))
      .thenReturn(Utils.getApiJson("history_task_attempts.json"));
    testSubject.addFinishedAttemptInfo(entities.RUNNING_REDUCE_TASK);
    assertThat(entities.RUNNING_REDUCE_TASK, is(entities.FINISHED_DETAILED_REDUCE_TASK));
  }

  @Test
  public void jobCountersMappingTest() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    HadoopAPIClient.JobCountersWrapper counters = mapper.readValue(Utils.getApiJson("job_counters.json"), HadoopAPIClient.JobCountersWrapper.class);
    counters.jobCounters.setLastUpdated(entities.JOB_COUNTERS.getLastUpdated());
    assertThat(counters.jobCounters, is(entities.JOB_COUNTERS));
  }

  @Test
  public void taskCountersMappingTest() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    HadoopAPIClient.TaskCountersWrapper counters = mapper.readValue(Utils.getApiJson("task_counters.json"), HadoopAPIClient.TaskCountersWrapper.class);
    counters.jobTaskCounters.setLastUpdated(entities.TASK_COUNTERS.getLastUpdated());
    assertThat(counters.jobTaskCounters, is(entities.TASK_COUNTERS));
  }

  @Test
  public void getFinishedJobConfTest() throws Exception {

    when(restClient.getInfo(eq(String.class), eq(TrackingUI.HISTORY), eq("jobs/%s/conf"), any(String[].class)))
      .thenReturn("{}");
    JobConfProxy ret = testSubject.getFinishedJobConf(entities.JOB_ID);
    assertThat(ret, nullValue());

    when(restClient.getInfo(eq(String.class), eq(TrackingUI.HISTORY), eq("jobs/%s/conf"), any(String[].class)))
      .thenReturn(Utils.getApiJson("job_conf.json"));
    ret = testSubject.getFinishedJobConf(entities.JOB_ID);
    assertThat(ret.getId(), is(entities.JOB_ID));
    assertThat(ret.getPropertyMap().entrySet(), everyItem(isIn(entities.JOB_CONF.getPropertyMap().entrySet())));
  }

} 
