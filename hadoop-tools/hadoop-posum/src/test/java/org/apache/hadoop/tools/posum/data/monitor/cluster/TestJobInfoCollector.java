package org.apache.hadoop.tools.posum.data.monitor.cluster;

import org.apache.hadoop.mapreduce.split.JobSplit;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.tools.posum.client.data.Database;
import org.apache.hadoop.tools.posum.common.records.call.JobForAppCall;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.TaskProfile;
import org.apache.hadoop.tools.posum.common.records.payload.SingleEntityPayload;
import org.apache.hadoop.tools.posum.common.util.Utils;
import org.apache.hadoop.yarn.util.Records;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.JOB;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.JOB_CONF;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestJobInfoCollector {

  private final String[] LOCATIONS = new String[]{"someLocation"};
  private final long INPUT_LENGTH = 1L;

  @Mock
  private Database dbMock;
  @Mock
  private HadoopAPIClient apiMock;
  @Mock
  private HdfsReader hdfsReaderMock;

  @InjectMocks
  private JobInfoCollector testSubject = new JobInfoCollector();

  private ClusterMonitorEntities entities;

  @Before
  public void init() {
    entities = new ClusterMonitorEntities();
  }

  @Test
  public void getExistingRunningJobInfoTest() {
    when(dbMock.execute(any(JobForAppCall.class)))
      .thenReturn(SingleEntityPayload.newInstance(DataEntityCollection.JOB, entities.RUNNING_JOB));
    when(apiMock.getRunningJobInfo(entities.APP_ID, entities.RUNNING_APP.getQueue(), entities.RUNNING_JOB)).thenReturn(entities.RUNNING_JOB);
    when(apiMock.getRunningJobCounters(entities.APP_ID, entities.JOB_ID)).thenReturn(entities.JOB_COUNTERS);

    JobInfo ret = testSubject.getRunningJobInfo(entities.RUNNING_APP);

    assertThat(ret, is(new JobInfo(entities.RUNNING_JOB, null, entities.JOB_COUNTERS)));
  }

  @Test
  public void getNewRunningJobInfoTest() throws IOException {
    JobId jobId = Utils.parseJobId(entities.JOB_ID);
    when(hdfsReaderMock.getSubmittedConf(jobId, entities.RUNNING_APP.getUser())).thenReturn(entities.JOB_CONF);
    when(hdfsReaderMock.getSplitMetaInfo(jobId, entities.JOB_CONF))
      .thenReturn(new JobSplit.TaskSplitMetaInfo[]{new JobSplit.TaskSplitMetaInfo(null, LOCATIONS, INPUT_LENGTH)});
    when(dbMock.execute(any(JobForAppCall.class))).thenReturn(null);
    when(apiMock.getRunningJobInfo(eq(entities.APP_ID), eq(entities.RUNNING_APP.getQueue()), any(JobProfile.class)))
      .thenReturn(entities.RUNNING_JOB);
    when(apiMock.getRunningJobCounters(entities.APP_ID, entities.JOB_ID)).thenReturn(entities.JOB_COUNTERS);

    JobInfo ret = testSubject.getRunningJobInfo(entities.RUNNING_APP);

    assertThat(ret, is(new JobInfo(entities.RUNNING_JOB, entities.JOB_CONF, entities.JOB_COUNTERS)));
  }

  @Test
  public void getFinishedJobInfoTest() {
    when(dbMock.execute(any(JobForAppCall.class))).thenReturn(SingleEntityPayload.newInstance(JOB, entities.RUNNING_JOB));
    when(apiMock.getFinishedJobInfo(entities.APP_ID)).thenReturn(entities.FINISHED_JOB);
    when(apiMock.getFinishedJobInfo(entities.APP_ID, entities.JOB_ID, entities.RUNNING_JOB)).thenReturn(entities.FINISHED_JOB);
    when(apiMock.getFinishedJobConf(entities.JOB_ID)).thenReturn(entities.JOB_CONF);
    when(apiMock.getFinishedJobCounters(entities.JOB_ID)).thenReturn(entities.JOB_COUNTERS);

    JobInfo ret = testSubject.getFinishedJobInfo(entities.FINISHED_APP);

    assertThat(ret, is(new JobInfo(entities.FINISHED_JOB, entities.JOB_CONF, entities.JOB_COUNTERS)));
  }

  @Test
  public void getSubmittedJobInfoTest() throws IOException {
    JobId jobId = Utils.parseJobId(entities.JOB_ID);
    when(hdfsReaderMock.getSubmittedConf(jobId, entities.RUNNING_APP.getUser())).thenReturn(entities.JOB_CONF);
    when(hdfsReaderMock.getSplitMetaInfo(jobId, entities.JOB_CONF))
      .thenReturn(new JobSplit.TaskSplitMetaInfo[]{new JobSplit.TaskSplitMetaInfo(null, LOCATIONS, INPUT_LENGTH)});
    JobInfo ret = testSubject.getSubmittedJobInfo(entities.APP_ID, entities.RUNNING_APP.getUser());

    ret.getProfile().setLastUpdated(entities.RUNNING_JOB.getLastUpdated());

    JobProfile expectedJob = entities.RUNNING_JOB.copy();
    expectedJob.setStartTime(null);
    expectedJob.setFinishTime(null);
    expectedJob.setState(null);
    expectedJob.setMapProgress(null);
    expectedJob.setReduceProgress(null);
    expectedJob.setCompletedMaps(null);
    expectedJob.setCompletedReduces(null);
    expectedJob.setUberized(null);
    expectedJob.setInputSplits(LOCATIONS.length);
    expectedJob.setTotalSplitSize(INPUT_LENGTH);
    expectedJob.setAggregatedSplitLocations(new HashSet<>(Arrays.asList(LOCATIONS)));
    expectedJob.setMapperClass(entities.JOB_CONF.getEntry("mapreduce.job.map.class"));
    expectedJob.setReducerClass(entities.JOB_CONF.getEntry("mapreduce.job.reduce.class"));
    expectedJob.setDeadline(1326381330000L);

    TaskProfile expectedMap = Records.newRecord(TaskProfile.class);
    expectedMap.setId(entities.RUNNING_MAP_TASK.getId());
    expectedMap.setType(TaskType.MAP);
    expectedMap.setJobId(expectedJob.getId());
    expectedMap.setAppId(expectedJob.getAppId());
    expectedMap.setSplitSize(INPUT_LENGTH);
    expectedMap.setSplitLocations(Arrays.asList(LOCATIONS));

    TaskProfile expectedReduce = Records.newRecord(TaskProfile.class);
    expectedReduce.setId(entities.RUNNING_REDUCE_TASK.getId());
    expectedReduce.setType(TaskType.REDUCE);
    expectedReduce.setJobId(expectedJob.getId());
    expectedReduce.setAppId(expectedJob.getAppId());

    assertThat(ret, is(new JobInfo(expectedJob, entities.JOB_CONF, Arrays.asList(expectedMap, expectedReduce))));
  }


} 
