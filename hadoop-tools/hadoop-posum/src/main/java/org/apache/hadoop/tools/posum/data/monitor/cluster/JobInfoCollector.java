package org.apache.hadoop.tools.posum.data.monitor.cluster;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.split.JobSplit;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.tools.posum.client.data.Database;
import org.apache.hadoop.tools.posum.common.records.call.JobForAppCall;
import org.apache.hadoop.tools.posum.common.records.dataentity.AppProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.CountersProxy;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobConfProxy;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.payload.SingleEntityPayload;
import org.apache.hadoop.tools.posum.common.util.PosumException;
import org.apache.hadoop.tools.posum.common.util.Utils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.Records;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

class JobInfoCollector {
    private static Log logger = LogFactory.getLog(JobInfoCollector.class);

    private static final String OLD_MAP_CLASS_ATTR = "mapred.mapper.class";
    private static final String OLD_REDUCE_CLASS_ATTR = "mapred.reducer.class";

    private HadoopAPIClient api;
    private Database db;
    private HdfsReader hdfsReader;

    JobInfoCollector(){

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
        // update job
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
        setClassNamesFromConf(profile, jobConf);
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
            // update the running info with the history info
            job = api.getFinishedJobInfo(appId, job.getId(), job);
        }
        return job;
    }

    private JobProfile getCurrentProfileForApp(AppProfile app) {
        SingleEntityPayload ret = db.executeDatabaseCall(JobForAppCall.newInstance(app.getId(), app.getUser()));
        if (ret == null)
            return null;
        return ret.getEntity();
    }

    JobInfo getSubmittedJobInfo(String appId,
                                String user) throws IOException {
        ApplicationId realAppId = Utils.parseApplicationId(appId);
        JobId jobId = Utils.composeJobId(realAppId.getClusterTimestamp(), realAppId.getId());
        final JobConfProxy confProxy = hdfsReader.getSubmittedConf(jobId, user);
        final JobProfile job = getJobProfileFromConf(jobId, confProxy);
        job.setTotalMapTasks(job.getInputSplits());
        int reduces = 0;
        String reducesString = confProxy.getEntry(MRJobConfig.NUM_REDUCES);
        if (reducesString != null && reducesString.length() > 0)
            reduces = Integer.valueOf(reducesString);
        job.setTotalReduceTasks(reduces);
        return new JobInfo(job, confProxy, null);
    }

    private JobProfile getJobProfileFromConf(JobId jobId, JobConfProxy jobConfProxy) throws IOException {
        JobSplit.TaskSplitMetaInfo[] taskSplitMetaInfo = hdfsReader.getSplitMetaInfo(jobId, jobConfProxy);
        long inputLength = 0;
        List<String> splitLocations = new ArrayList<>(taskSplitMetaInfo.length);
        for (JobSplit.TaskSplitMetaInfo aTaskSplitMetaInfo : taskSplitMetaInfo) {
            inputLength += aTaskSplitMetaInfo.getInputDataLength();
            splitLocations.add(StringUtils.join(" ", aTaskSplitMetaInfo.getLocations()));
        }

        JobProfile profile = Records.newRecord(JobProfile.class);
        profile.setId(jobId.toString());
        profile.setAppId(jobId.getAppId().toString());
        profile.setName(jobConfProxy.getEntry(MRJobConfig.JOB_NAME));
        profile.setUser(jobConfProxy.getEntry(MRJobConfig.USER_NAME));
        profile.setQueue(jobConfProxy.getEntry(MRJobConfig.QUEUE_NAME));
        profile.setTotalInputBytes(inputLength);
        profile.setInputSplits(taskSplitMetaInfo.length);
        setClassNamesFromConf(profile, jobConfProxy);
        profile.setSplitLocations(splitLocations);
        return profile;
    }

    private void setClassNamesFromConf(JobProfile profile, JobConfProxy conf) {
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
