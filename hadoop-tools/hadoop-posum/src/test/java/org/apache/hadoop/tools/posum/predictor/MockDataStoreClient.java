package org.apache.hadoop.tools.posum.predictor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.tools.posum.database.DataStoreClient;
import org.apache.hadoop.tools.posum.database.records.JobProfile;
import org.apache.hadoop.tools.posum.database.records.TaskProfile;
import org.apache.hadoop.tools.rumen.JobTraceReader;
import org.apache.hadoop.tools.rumen.LoggedJob;
import org.apache.hadoop.tools.rumen.LoggedTask;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by ane on 2/10/16.
 */
public class MockDataStoreClient extends DataStoreClient {

    private List<JobProfile> jobList = new ArrayList<>();
    private long simulationTime = 0;
    private long currentTime = 0;

    public List<JobProfile> getJobList() {
        return jobList;
    }

    public long getSimulationTime() {
        return simulationTime;
    }

    public long getCurrentTime() {
        return currentTime;
    }

    public void setCurrentTime(long currentTime) {
        this.currentTime = currentTime;
    }

    private TaskProfile buildTaskProfile(LoggedTask task, long startTime) {
        TaskProfile profile = new TaskProfile(task.getTaskID(),
                TaskType.valueOf(task.getTaskType().name()));
        profile.setStartTime(task.getStartTime() - startTime);
        profile.setFinishTime(task.getFinishTime() - startTime);
        profile.setStartTime(task.getStartTime());
        profile.setInputBytes(task.getInputBytes());
        profile.setInputRecords(task.getInputRecords());
        profile.setOutputBytes(task.getOutputBytes());
        profile.setOutputRecords(task.getOutputRecords());
        //TODO continue with other task characteristics (!attempts)
        return profile;
    }

    public void populateFromTrace(String inputTrace) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");

        File fin = new File(inputTrace);
        JobTraceReader reader = new JobTraceReader(
                new Path(fin.getAbsolutePath()), conf);
        LoggedJob job;
        long startTime = 0;
        while ((job = reader.getNext()) != null) {

            JobID jobId = job.getJobID();

            long jobStartTimeMS = job.getSubmitTime();
            long jobFinishTimeMS = job.getFinishTime();
            if (startTime == 0) {
                startTime = jobStartTimeMS;
            }
            jobStartTimeMS -= startTime;
            jobFinishTimeMS -= startTime;
            if (jobStartTimeMS < 0) {
                jobFinishTimeMS = jobFinishTimeMS - jobStartTimeMS;
                jobStartTimeMS = 0;
            }

            JobProfile profile = new JobProfile(jobId, jobStartTimeMS);
            profile.setFinishTime(jobFinishTimeMS);
            profile.setUser(job.getUser() == null ?
                    "default" : job.getUser().getValue());
            profile.setQueue(job.getQueue().getValue());
            profile.setTotalMapTasks(job.getTotalMaps());
            profile.setTotalReduceTasks(job.getTotalReduces());
            profile.setJobName(job.getJobName().getValue());
            //TODO continue with other job characteristics (look into computonsperbyte)

            for (LoggedTask task : job.getMapTasks())
                profile.recordTask(buildTaskProfile(task, startTime));
            for (LoggedTask task : job.getReduceTasks())
                profile.recordTask(buildTaskProfile(task, startTime));

            jobList.add(profile);

            if (jobFinishTimeMS > simulationTime)
                simulationTime = jobFinishTimeMS;
        }
    }


    @Override
    public TaskProfile getTaskProfile(String taskId) {
        return super.getTaskProfile(taskId);
    }

    @Override
    public JobProfile getJobProfile(String jobId) {
        return super.getJobProfile(jobId);
    }

    @Override
    public List<JobProfile> getComparableProfiles(String user, int count) {

        return super.getComparableProfiles(user, count);
    }
}
