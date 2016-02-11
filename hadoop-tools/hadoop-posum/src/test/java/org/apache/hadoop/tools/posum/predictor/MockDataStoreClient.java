package org.apache.hadoop.tools.posum.predictor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.tools.posum.database.DataStoreClient;
import org.apache.hadoop.tools.posum.database.records.JobProfile;
import org.apache.hadoop.tools.posum.database.records.TaskProfile;
import org.apache.hadoop.tools.rumen.JobTraceReader;
import org.apache.hadoop.tools.rumen.LoggedJob;
import org.apache.hadoop.tools.rumen.LoggedTask;

import java.io.File;
import java.io.IOException;
import java.util.*;

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

            JobProfile profile = new JobProfile(jobId, job.getSubmitTime() - startTime);
            profile.populate(job.getJobName().getValue(),
                    job.getUser() == null ? "default" : job.getUser().getValue(),
                    job.getQueue().getValue(),
                    job.getTotalMaps(),
                    job.getTotalReduces(),
                    jobStartTimeMS,
                    jobFinishTimeMS
            );
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
    public TaskProfile getTaskProfile(TaskID taskId) {
        JobID parent = taskId.getJobID();
        return getJobProfile(parent).getTask(taskId);
    }

    private JobProfile snapshot(JobProfile original) {
        JobProfile copy = new JobProfile(original.getJobId(), original.getSubmitTime());
        copy.populate(
                original.getJobName(),
                original.getUser() == null ? "default" : original.getUser(),
                original.getQueue(),
                original.getTotalMapTasks(),
                original.getTotalReduceTasks(),
                original.getStartTime() > currentTime ? null : original.getStartTime(),
                original.getFinishTime() > currentTime ? null : original.getFinishTime()
        );
        //TODO copy all tasks with obfuscated times
        return copy;
    }

    @Override
    public JobProfile getJobProfile(JobID jobId) {
        for (JobProfile job : jobList)
            if (job.getJobId().equals(jobId))
                return snapshot(job);
        return null;
    }

    private void storeIfMoreRecent(JobProfile job, TreeMap<Long, JobProfile> list) {
        if (list.firstKey() < job.getFinishTime()) {
            list.remove(list.firstKey());
            list.put(job.getFinishTime(), job);
        }
    }

    @Override
    public List<JobProfile> getComparableProfiles(String user, int count) {
        TreeMap<Long, JobProfile> latest = new TreeMap<>();
        TreeMap<Long, JobProfile> relevant = new TreeMap<>();
        for (JobProfile job : jobList) {
            if (job.getFinishTime() <= currentTime) {
                if (job.getUser().equals(user))
                    storeIfMoreRecent(job, relevant);
                else
                    storeIfMoreRecent(job, latest);
            }
        }
        List<JobProfile> ret = new ArrayList<>(count);
        ret.addAll(relevant.values());
        Iterator<JobProfile> latestIterator = latest.values().iterator();
        for (int i = ret.size(); i < count && latestIterator.hasNext(); i++) {
            ret.add(latestIterator.next());
        }
        return ret;
    }

    public Map<JobID, List<TaskID>> getFutureJobInfo() {
        Map<JobID, List<TaskID>> ret = new HashMap<>(jobList.size());
        for (JobProfile job : jobList)
            if (job.getFinishTime() <= currentTime) {
                List<TaskID> tasks = new ArrayList<>(job.getTotalMapTasks() + job.getTotalReduceTasks());
                tasks.addAll(job.getMapTasks().keySet());
                tasks.addAll(job.getReduceTasks().keySet());
                ret.put(job.getJobId(), tasks);
            }
        return ret;
    }
}
