package org.apache.hadoop.tools.posum.data.monitor.cluster;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.split.JobSplit;
import org.apache.hadoop.mapreduce.split.SplitMetaInfoReader;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.tools.posum.client.data.Database;
import org.apache.hadoop.tools.posum.common.records.call.JobForAppCall;
import org.apache.hadoop.tools.posum.common.records.dataentity.AppProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.CountersProxy;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobConfProxy;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.util.PosumException;
import org.apache.hadoop.tools.posum.common.util.Utils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.Records;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

class JobInfoCollector {
    private static Log logger = LogFactory.getLog(JobInfoCollector.class);

    private static final String OLD_MAP_CLASS_ATTR = "mapred.mapper.class";
    private static final String OLD_REDUCE_CLASS_ATTR = "mapred.reducer.class";

    private Configuration conf;
    private HadoopAPIClient api;
    private Database db;

    JobInfoCollector(Configuration conf, HadoopAPIClient api, Database db) {
        this.conf = conf;
        this.api = api;
        this.db = db;
    }

    JobInfo getRunningJobInfo(AppProfile app) {
        JobInfo info = new JobInfo();
        JobProfile profile = getCurrentProfileForApp(app);
        if (profile == null) {
            // if not found, force the reading of the configuration
            try {
                logger.debug("Forcing fetch of job info from conf for " + app.getId());
                info = getSubmittedJobInfo(app.getId(), app.getUser());
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
        CountersProxy counters =  api.getRunningJobCounters(app.getId(), profile.getId());
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
        JobForAppCall getJobForApp = JobForAppCall.newInstance(appId, app.getUser());
        JobProfile job = db.executeDatabaseCall(getJobForApp).getEntity();
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
        return db.executeDatabaseCall(JobForAppCall.newInstance(app.getId(), app.getUser())).getEntity();
    }

    JobInfo getSubmittedJobInfo(String appId,
                                String user) throws IOException {
        ApplicationId realAppId = Utils.parseApplicationId(appId);
        final JobConfProxy confProxy = getSubmittedConf(realAppId, user);
        final JobProfile job = getSubmittedJobProfile(confProxy, realAppId);
        job.setTotalMapTasks(job.getInputSplits());
        int reduces = 0;
        String reducesString = confProxy.getEntry(MRJobConfig.NUM_REDUCES);
        if (reducesString != null && reducesString.length() > 0)
            reduces = Integer.valueOf(reducesString);
        job.setTotalReduceTasks(reduces);
        return new JobInfo(job, confProxy, null);
    }

    private JobConfProxy getSubmittedConf(ApplicationId appId, String user) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path confPath = MRApps.getStagingAreaDir(conf,
                user != null ? user : UserGroupInformation.getCurrentUser().getUserName());
        confPath = fs.makeQualified(confPath);

        //DANGER We assume there can only be one job / application
        Path jobConfDir = new Path(confPath,
                Utils.composeJobId(appId.getClusterTimestamp(), appId.getId()).toString());
        logger.debug("Checking file path for conf: " + jobConfDir);
        Path jobConfPath = new Path(jobConfDir, "job.xml");
        Configuration jobConf = new JobConf(false);
        jobConf.addResource(fs.open(jobConfPath), jobConfPath.toString());
        JobConfProxy proxy = Records.newRecord(JobConfProxy.class);
        proxy.setId(jobConfDir.getName());
        proxy.setConfPath(jobConfDir.toUri().toString());
        proxy.setConf(jobConf);
        return proxy;
    }

    private JobProfile getSubmittedJobProfile(JobConfProxy confProxy, ApplicationId appId) throws IOException {
        FileSystem fs = FileSystem.get(confProxy.getConf());
        String jobConfDirPath = confProxy.getConfPath();
        Path jobConfDir;
        try {
            jobConfDir = new Path(new URI(jobConfDirPath));
            //DANGER We assume there can only be one job / application
            return getJobProfileFromConf(
                    appId,
                    Utils.composeJobId(appId.getClusterTimestamp(), appId.getId()),
                    fs,
                    confProxy,
                    jobConfDir
            );
        } catch (URISyntaxException e) {
            throw new PosumException("Invalid jobConfDir path " + jobConfDirPath, e);
        }
    }

    private JobProfile getJobProfileFromConf(ApplicationId appId,
                                             JobId jobId,
                                             FileSystem fs,
                                             JobConfProxy jobConfProxy,
                                             Path jobSubmitDir) throws IOException {
        JobSplit.TaskSplitMetaInfo[] taskSplitMetaInfo = SplitMetaInfoReader.readSplitMetaInfo(
                TypeConverter.fromYarn(jobId), fs,
                jobConfProxy.getConf(),
                jobSubmitDir);

        long inputLength = 0;
        List<String> splitLocations = new ArrayList<>(taskSplitMetaInfo.length);
        for (JobSplit.TaskSplitMetaInfo aTaskSplitMetaInfo : taskSplitMetaInfo) {
            inputLength += aTaskSplitMetaInfo.getInputDataLength();
            splitLocations.add(StringUtils.join(" ", aTaskSplitMetaInfo.getLocations()));
        }

        JobProfile profile = Records.newRecord(JobProfile.class);
        profile.setId(jobId.toString());
        profile.setAppId(appId.toString());
        profile.setName(jobConfProxy.getEntry(MRJobConfig.JOB_NAME));
        profile.setUser(jobConfProxy.getEntry(MRJobConfig.USER_NAME));
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
