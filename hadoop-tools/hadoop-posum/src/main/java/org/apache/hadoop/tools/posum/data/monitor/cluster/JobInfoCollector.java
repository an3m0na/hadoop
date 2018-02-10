package org.apache.hadoop.tools.posum.data.monitor.cluster;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.split.JobSplit;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.util.MRBuilderUtils;
import org.apache.hadoop.tools.posum.client.data.Database;
import org.apache.hadoop.tools.posum.common.records.call.FindByIdCall;
import org.apache.hadoop.tools.posum.common.records.call.JobForAppCall;
import org.apache.hadoop.tools.posum.common.records.dataentity.AppProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.CountersProxy;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.ExternalDeadline;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobConfProxy;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.payload.SingleEntityPayload;
import org.apache.hadoop.tools.posum.common.util.PosumException;
import org.apache.hadoop.tools.posum.common.util.cluster.ClusterUtils;
import org.apache.hadoop.tools.posum.common.util.communication.RestClient;
import org.apache.hadoop.tools.posum.common.util.conf.PosumConfiguration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.Records;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.hadoop.tools.posum.common.util.conf.PosumConfiguration.DATABASE_DEADLINES;
import static org.apache.hadoop.tools.posum.common.util.conf.PosumConfiguration.DATABASE_DEADLINES_DEFAULT;

class JobInfoCollector {
  private static Log logger = LogFactory.getLog(JobInfoCollector.class);

  private static final String OLD_MAP_CLASS_ATTR = "mapred.mapper.class";
  private static final String OLD_REDUCE_CLASS_ATTR = "mapred.reducer.class";

  private HadoopAPIClient api;
  private Database db;
  private HdfsReader hdfsReader;
  private boolean databaseDeadlines;

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
    this.databaseDeadlines = conf.getBoolean(DATABASE_DEADLINES, DATABASE_DEADLINES_DEFAULT);
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
    if (RestClient.TrackingUI.AM.equals(app.getTrackingUI())) {
      JobProfile newProfile = api.getRunningJobInfo(app.getId(), app.getQueue(), profile);
      if (newProfile == null)
        // job might have finished; return
        return null;
      profile = newProfile;
      if (!api.addRunningAttemptInfo(profile)) {
        return null;
      }
      // get counters
      CountersProxy counters = api.getRunningJobCounters(app.getId(), profile.getId());
      if (counters == null)
        // job might have finished; return
        return null;
      info.setJobCounters(counters);
    }
    if (profile == null)
      return null;
    if (profile.getSubmitTime() == null)
      profile.setSubmitTime(app.getStartTime());
    if (databaseDeadlines && profile.getDeadline() == null) {
      setDatabaseDeadline(profile);
    }
    info.setProfile(profile);
    return info;
  }

  JobInfo getFinishedJobInfo(AppProfile app) {
    JobInfo info = new JobInfo();

    JobProfile profile = getFinishedJobProfile(app);
    info.setProfile(profile);
    api.addFinishedAttemptInfo(profile);

    JobConfProxy jobConf = api.getFinishedJobConf(profile.getId());
    setClassNames(profile, jobConf);
    if (profile.getDeadline() == null) {
      if (databaseDeadlines)
        setDatabaseDeadline(profile);
      else
        setDeadlineFromConf(profile, jobConf);
    }
    info.setConf(jobConf);

    CountersProxy counters = api.getFinishedJobCounters(profile.getId());
    info.setJobCounters(counters);
    ClusterUtils.updateJobStatisticsFromCounters(profile, counters);

    return info;
  }

  private JobProfile getFinishedJobProfile(AppProfile app) {
    String appId = app.getId();
    JobProfile job = getCurrentProfileForApp(app);
    if (job == null) {
      // there is no running record of the job
      job = api.getFinishedJobInfo(appId);
    } else {
      // readStatsFromFlexFields the running info with the history info
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
    ApplicationId realAppId = ClusterUtils.parseApplicationId(appId);
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
    if (!databaseDeadlines)
      setDeadlineFromConf(job, jobConfProxy);

    // read split info
    JobSplit.TaskSplitMetaInfo[] taskSplitMetaInfo = hdfsReader.getSplitMetaInfo(jobId, jobConfProxy);
    job.setTotalMapTasks(taskSplitMetaInfo.length);

    // add one map task stub per split
    long inputLength = 0;
    List<List<String>> splitLocations = new ArrayList<>(taskSplitMetaInfo.length);
    List<Long> splitSizes = new ArrayList<>(taskSplitMetaInfo.length);
    for (int i = 0; i < taskSplitMetaInfo.length; i++) {
      JobSplit.TaskSplitMetaInfo aTaskSplitMetaInfo = taskSplitMetaInfo[i];
      long splitSize = aTaskSplitMetaInfo.getInputDataLength();
      inputLength += splitSize;
      splitSizes.add(splitSize);
      splitLocations.add(Arrays.asList(aTaskSplitMetaInfo.getLocations()));
    }
    job.setTotalSplitSize(inputLength);
    job.setSplitLocations(splitLocations);
    job.setSplitSizes(splitSizes);

    // add reduce task stubs according to configuration
    int reduces = 0;
    String reducesString = jobConfProxy.getEntry(MRJobConfig.NUM_REDUCES);
    if (reducesString != null && reducesString.length() > 0)
      reduces = Integer.valueOf(reducesString);
    job.setTotalReduceTasks(reduces);

    return new JobInfo(job, jobConfProxy);
  }

  private void setDeadlineFromConf(JobProfile job, JobConfProxy confProxy) {
    String deadlineString = confProxy.getEntry(PosumConfiguration.APP_DEADLINE);
    if (deadlineString != null)
      job.setDeadline(Long.valueOf(deadlineString));
    else
      job.setDeadline(0L);
  }

  private void setDatabaseDeadline(JobProfile job) {
    FindByIdCall findDeadline = FindByIdCall.newInstance(DataEntityCollection.DEADLINE, job.getId());
    ExternalDeadline deadline = db.execute(findDeadline).getEntity();
    if (deadline != null)
      job.setDeadline(deadline.getDeadline());
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
