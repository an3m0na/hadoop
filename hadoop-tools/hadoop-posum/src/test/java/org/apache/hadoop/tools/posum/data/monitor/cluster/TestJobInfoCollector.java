package org.apache.hadoop.tools.posum.data.monitor.cluster;

import org.apache.hadoop.mapreduce.split.JobSplit;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.tools.posum.client.data.Database;
import org.apache.hadoop.tools.posum.common.records.call.JobForAppCall;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.payload.SingleEntityPayload;
import org.apache.hadoop.tools.posum.common.util.cluster.ClusterUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;

import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.JOB;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestJobInfoCollector {

  @Mock
  private Database dbMock;
  @Mock
  private HadoopAPIClient apiMock;
  @Mock
  private HdfsReader hdfsReaderMock;

  @InjectMocks
  private JobInfoCollector testSubject = new JobInfoCollector();

  private ClusterMonitorEntities entities;
  private JobSplit.TaskSplitMetaInfo[] taskSplitMetaInfos;

  @Before
  public void init() {
    entities = new ClusterMonitorEntities();
     taskSplitMetaInfos = new JobSplit.TaskSplitMetaInfo[] {new JobSplit.TaskSplitMetaInfo(null, entities.SPLIT_LOCATIONS.get(0).toArray(new String[]{}), entities.SPLIT_SIZES.get(0))};
  }

  @Test
  public void getExistingRunningJobInfoTest() {
    when(dbMock.execute(any(JobForAppCall.class)))
      .thenReturn(SingleEntityPayload.newInstance(DataEntityCollection.JOB, entities.RUNNING_JOB));
    when(apiMock.getRunningJobInfo(entities.APP_ID, entities.RUNNING_APP.getQueue(), entities.RUNNING_JOB)).thenReturn(entities.RUNNING_JOB);
    when(apiMock.addRunningAttemptInfo(entities.RUNNING_JOB)).thenReturn(true);
    when(apiMock.getRunningJobCounters(entities.APP_ID, entities.JOB_ID)).thenReturn(entities.JOB_COUNTERS);

    JobInfo ret = testSubject.getRunningJobInfo(entities.RUNNING_APP);

    assertThat(ret, is(new JobInfo(entities.RUNNING_JOB, null, entities.JOB_COUNTERS)));
  }

  @Test
  public void getNewRunningJobInfoTest() throws IOException {
    JobId jobId = ClusterUtils.parseJobId(entities.JOB_ID);
    when(hdfsReaderMock.getSubmittedConf(jobId, entities.RUNNING_APP.getUser())).thenReturn(entities.JOB_CONF);
    when(hdfsReaderMock.getSplitMetaInfo(jobId, entities.JOB_CONF)).thenReturn(taskSplitMetaInfos);
    when(dbMock.execute(any(JobForAppCall.class))).thenReturn(null);
    when(apiMock.getRunningJobInfo(eq(entities.APP_ID), eq(entities.RUNNING_APP.getQueue()), any(JobProfile.class)))
      .thenReturn(entities.RUNNING_JOB);
    when(apiMock.addRunningAttemptInfo(entities.RUNNING_JOB)).thenReturn(true);
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
    JobId jobId = ClusterUtils.parseJobId(entities.JOB_ID);
    when(hdfsReaderMock.getSubmittedConf(jobId, entities.RUNNING_APP.getUser())).thenReturn(entities.JOB_CONF);
    when(hdfsReaderMock.getSplitMetaInfo(jobId, entities.JOB_CONF)).thenReturn(taskSplitMetaInfos);
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
    expectedJob.setMapperClass(entities.JOB_CONF.getEntry("mapreduce.job.map.class"));
    expectedJob.setReducerClass(entities.JOB_CONF.getEntry("mapreduce.job.reduce.class"));
    expectedJob.setDeadline(1326381330000L);

    assertThat(ret, is(new JobInfo(expectedJob, entities.JOB_CONF)));
  }


} 
