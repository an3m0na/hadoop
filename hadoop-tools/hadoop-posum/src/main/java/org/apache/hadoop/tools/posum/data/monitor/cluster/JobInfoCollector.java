package org.apache.hadoop.tools.posum.data.monitor.cluster;

import com.mongodb.DuplicateKeyException;
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
import org.apache.hadoop.tools.posum.common.records.call.StoreCall;
import org.apache.hadoop.tools.posum.common.records.call.TransactionCall;
import org.apache.hadoop.tools.posum.common.records.dataentity.AppProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.CountersProxy;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
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

import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.JOB;

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

    JobProfile getRunningJobInfo(AppProfile app) {
        JobProfile lastJobInfo = getCurrentProfileForApp(app.getId(), app.getUser());
        return api.getRunningJobInfo(app.getId(), app.getQueue(), lastJobInfo);
    }

    JobProfile getFinishedJobInfo(AppProfile app) {
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

    private JobProfile getCurrentProfileForApp(String appId, String user) {
        JobProfile profile = db.executeDatabaseCall(JobForAppCall.newInstance(appId, user)).getEntity();
        if (profile != null)
            return profile;
        // if not found, force the reading of the configuration
        try {
            logger.debug("Forcing fetch of job info from conf for " + appId);
            return getAndStoreSubmittedJobInfo(conf, appId, user, db);
        } catch (Exception e) {
            logger.debug("Could not retrieve job info for app " + appId, e);
        }
        return null;
    }

    JobProfile getAndStoreSubmittedJobInfo(Configuration conf,
                                           String appId,
                                           String user,
                                           Database db) throws IOException {
        ApplicationId realAppId = Utils.parseApplicationId(appId);
        final JobConfProxy confProxy = getSubmittedJobConf(conf, realAppId, user);
        final JobProfile job = getSubmittedJobInfo(confProxy, realAppId);
        job.setTotalMapTasks(job.getInputSplits());
        int reduces = 0;
        String reducesString = confProxy.getEntry(MRJobConfig.NUM_REDUCES);
        if (reducesString != null && reducesString.length() > 0)
            reduces = Integer.valueOf(reducesString);
        job.setTotalReduceTasks(reduces);
        TransactionCall transaction = TransactionCall.newInstance();
        transaction.addCall(StoreCall.newInstance(JOB, job))
                .addCall(StoreCall.newInstance(DataEntityCollection.JOB_CONF, confProxy));
        try {
            db.executeDatabaseCall(transaction);
        } catch (DuplicateKeyException e) {
            // this is possible; do nothing
        } catch (Exception e) {
            logger.warn("Exception occurred when storing new job info", e);
        }
        return job;
    }

    private static JobConfProxy getSubmittedJobConf(Configuration conf, final ApplicationId appId, String user) throws IOException {
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

    private static JobProfile getSubmittedJobInfo(JobConfProxy confProxy, ApplicationId appId) throws IOException {
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

    private static JobProfile getJobProfileFromConf(ApplicationId appId,
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

    private static void setClassNamesFromConf(JobProfile profile, JobConfProxy conf) {
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

    CountersProxy getRunningJobCounters(JobProfile job) {
        return api.getRunningJobCounters(job.getAppId(), job.getId());
    }

    JobConfProxy getFinishedJobConf(JobProfile job) {
        JobConfProxy jobConf = api.getFinishedJobConf(job.getId());
        setClassNamesFromConf(job, jobConf);
        return jobConf;
    }

    CountersProxy updateFinishedJobFromCounters(JobProfile job) {
        CountersProxy jobCounters = api.getFinishedJobCounters(job.getId());
        Utils.updateJobStatisticsFromCounters(job, jobCounters);
        return jobCounters;
    }
}
