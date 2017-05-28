package org.apache.hadoop.tools.posum.data.monitor.cluster;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.split.JobSplit;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.util.MRBuilderUtils;
import org.apache.hadoop.tools.posum.client.data.Database;
import org.apache.hadoop.tools.posum.common.records.call.JobForAppCall;
import org.apache.hadoop.tools.posum.common.records.dataentity.AppProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.CountersProxy;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobConfProxy;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.TaskProfile;
import org.apache.hadoop.tools.posum.common.records.payload.SingleEntityPayload;
import org.apache.hadoop.tools.posum.common.util.PosumConfiguration;
import org.apache.hadoop.tools.posum.common.util.PosumException;
import org.apache.hadoop.tools.posum.common.util.Utils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.Records;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

class JobInfoCollector {
  private static Log logger = LogFactory.getLog(JobInfoCollector.class);

  private static final String OLD_MAP_CLASS_ATTR = "mapred.mapper.class";
  private static final String OLD_REDUCE_CLASS_ATTR = "mapred.reducer.class";

  private HadoopAPIClient api;
  private Database db;
  private HdfsReader hdfsReader;

  JobInfoCollector() {

  }

  JobInfoCollector(Configuration conf, Database db) {
    this.api = new HadoopAPIClient();
    this.db = db;
    try {
      this.hdfsReader = new HdfsReader(conf);
    } catch (IOException e) {
      throw new PosumException("Cannot not access HDFS ", e);
    }
  }

  JobInfo getRunningJobInfo(AppProfile app) {
    JobInfo info = new JobInfo();
    JobProfile profile = getCurrentProfileForApp(app);
    if (profile == null) {
      // if not found, force the reading of the configuration
      try {
        logger.debug("Forcing fetch of job info from conf for " + app.getId());
        info = getSubmittedJobInfo(app.getId(), app.getUser());
        if (info.getProfile() != null)
          profile = info.getProfile();
      } catch (Exception e) {
        logger.debug("Could not retrieve job info for app " + app.getId(), e);
      }
    }
    // addSource job
    profile = api.getRunningJobInfo(app.getId(), app.getQueue(), profile);
    if (profile == null)
      // job might have finished; return
      return null;
    info.setProfile(profile);
    // get counters
    CountersProxy counters = api.getRunningJobCounters(app.getId(), profile.getId());
    if (counters == null)
      // job might have finished; return
      return null;
    info.setJobCounters(counters);
    return info;
  }

  JobInfo getFinishedJobInfo(AppProfile app) {
    JobInfo info = new JobInfo();

    JobProfile profile = getFinishedJobProfile(app);
    info.setProfile(profile);

    JobConfProxy jobConf = api.getFinishedJobConf(profile.getId());
    setClassNames(profile, jobConf);
    setDeadline(profile, jobConf);
    info.setConf(jobConf);

    CountersProxy counters = api.getFinishedJobCounters(profile.getId());
    info.setJobCounters(counters);
    Utils.updateJobStatisticsFromCounters(profile, counters);

    return info;
  }

  private JobProfile getFinishedJobProfile(AppProfile app) {
    String appId = app.getId();
    JobProfile job = getCurrentProfileForApp(app);
    if (job == null) {
      // there is no running record of the job
      job = api.getFinishedJobInfo(appId);
    } else {
      // addSource the running info with the history info
      job = api.getFinishedJobInfo(appId, job.getId(), job);
    }
    return job;
  }

  private JobProfile getCurrentProfileForApp(AppProfile app) {
    SingleEntityPayload ret = db.execute(JobForAppCall.newInstance(app.getId()));
    if (ret == null)
      return null;
    return ret.getEntity();
  }

  JobInfo getSubmittedJobInfo(String appId,
                              String user) throws IOException {
    ApplicationId realAppId = Utils.parseApplicationId(appId);
    JobId jobId = MRBuilderUtils.newJobId(realAppId, realAppId.getId());
    final JobConfProxy confProxy = hdfsReader.getSubmittedConf(jobId, user);
    return getJobInfoFromConf(jobId, confProxy);
  }

  private JobInfo getJobInfoFromConf(JobId jobId, JobConfProxy jobConfProxy) throws IOException {
    JobProfile job = Records.newRecord(JobProfile.class);
    job.setId(jobId.toString());
    job.setAppId(jobId.getAppId().toString());
    job.setName(jobConfProxy.getEntry(MRJobConfig.JOB_NAME));
    job.setUser(jobConfProxy.getEntry(MRJobConfig.USER_NAME));
    job.setQueue(jobConfProxy.getEntry(MRJobConfig.QUEUE_NAME));
    setClassNames(job, jobConfProxy);
    setDeadline(job, jobConfProxy);

    // read split info
    JobSplit.TaskSplitMetaInfo[] taskSplitMetaInfo = hdfsReader.getSplitMetaInfo(jobId, jobConfProxy);
    job.setInputSplits(taskSplitMetaInfo.length);
    job.setTotalMapTasks(taskSplitMetaInfo.length);

    // add one map task stub per split
    long inputLength = 0;
    Set<String> aggregatedLocations = new HashSet<>();
    List<TaskProfile> taskStubs = new ArrayList<>(taskSplitMetaInfo.length);
    for (int i = 0; i < taskSplitMetaInfo.length; i++) {
      JobSplit.TaskSplitMetaInfo aTaskSplitMetaInfo = taskSplitMetaInfo[i];
      inputLength += aTaskSplitMetaInfo.getInputDataLength();
      aggregatedLocations.addAll(Arrays.asList(aTaskSplitMetaInfo.getLocations()));
      TaskProfile task = Records.newRecord(TaskProfile.class);
      task.setId(MRBuilderUtils.newTaskId(jobId, i, TaskType.MAP).toString());
      task.setAppId(job.getAppId());
      task.setJobId(job.getId());
      task.setType(TaskType.MAP);
      task.setState(TaskState.NEW);
      task.setSplitLocations(Arrays.asList(aTaskSplitMetaInfo.getLocations()));
      task.setSplitSize(aTaskSplitMetaInfo.getInputDataLength());
      taskStubs.add(task);
    }
    job.setTotalSplitSize(inputLength);
    job.setAggregatedSplitLocations(aggregatedLocations);

    // add reduce task stubs according to configuration
    int reduces = 0;
    String reducesString = jobConfProxy.getEntry(MRJobConfig.NUM_REDUCES);
    if (reducesString != null && reducesString.length() > 0)
      reduces = Integer.valueOf(reducesString);
    job.setTotalReduceTasks(reduces);
    for (int i = 0; i < reduces; i++) {
      TaskProfile task = Records.newRecord(TaskProfile.class);
      task.setId(MRBuilderUtils.newTaskId(jobId, i, TaskType.REDUCE).toString());
      task.setAppId(job.getAppId());
      task.setJobId(job.getId());
      task.setType(TaskType.REDUCE);
      task.setState(TaskState.NEW);
      taskStubs.add(task);
    }

    return new JobInfo(job, jobConfProxy, taskStubs);
  }

  private void setDeadline(JobProfile job, JobConfProxy confProxy) {
    String deadlineString = confProxy.getEntry(PosumConfiguration.APP_DEADLINE);
    if (deadlineString != null)
      job.setDeadline(Long.valueOf(deadlineString));
  }

  private void setClassNames(JobProfile profile, JobConfProxy conf) {
    String classString = conf.getEntry(MRJobConfig.MAP_CLASS_ATTR);
    if (classString == null)
      classString = conf.getEntry(OLD_MAP_CLASS_ATTR);
    if (classString == null)
      profile.setReducerClass(IdentityMapper.class.getName());
    profile.setMapperClass(classString);
    classString = conf.getEntry(MRJobConfig.REDUCE_CLASS_ATTR);
    if (classString == null)
      classString = conf.getEntry(OLD_REDUCE_CLASS_ATTR);
    if (classString == null)
      profile.setReducerClass(IdentityReducer.class.getName());
    profile.setReducerClass(classString);
  }
}
