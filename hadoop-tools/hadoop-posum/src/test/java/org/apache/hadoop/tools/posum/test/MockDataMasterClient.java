package org.apache.hadoop.tools.posum.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.tools.posum.common.records.dataentity.*;
import org.apache.hadoop.tools.posum.common.util.Utils;
import org.apache.hadoop.tools.posum.database.client.DataMasterClient;
import org.apache.hadoop.tools.rumen.JobTraceReader;
import org.apache.hadoop.tools.rumen.LoggedJob;
import org.apache.hadoop.tools.rumen.LoggedTask;
import org.apache.hadoop.yarn.util.Records;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Created by ane on 2/10/16.
 */
public class MockDataMasterClient extends DataMasterClient {

    private List<JobProfile> jobList = new ArrayList<>();
    private Map<String, Map<String, TaskProfile>> taskMap = new HashMap<>();
    private long simulationTime = 0;
    private long currentTime = 0;

    public MockDataMasterClient() {
        super(null);
    }

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
        TaskProfile profile = Records.newRecord(TaskProfile.class);
        profile.setId(task.getTaskID().toString());
        profile.setStartTime(task.getStartTime() - startTime);
        profile.setFinishTime(task.getFinishTime() - startTime);
        profile.setInputBytes(task.getInputBytes());
        profile.setInputRecords(task.getInputRecords());
        profile.setOutputBytes(task.getOutputBytes());
        profile.setOutputRecords(task.getOutputRecords());
        profile.setJobId(task.getTaskID().getJobID().toString());
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

            String jobId = job.getJobID().toString();

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

            JobProfile profile = Records.newRecord(JobProfile.class);
            profile.setId(jobId);
            profile.setName(job.getJobName().getValue());
            profile.setUser(job.getUser() == null ? "default" : job.getUser().getValue());
            profile.setTotalMapTasks(job.getTotalMaps());
            profile.setTotalReduceTasks(job.getTotalReduces());
            profile.setStartTime(jobStartTimeMS);
            profile.setFinishTime(jobFinishTimeMS);
            //TODO continue with other job characteristics (look into computonsperbyte)

            Map<String, TaskProfile> taskList = new HashMap<>(job.getMapTasks().size() + job.getReduceTasks().size());
            for (LoggedTask task : job.getMapTasks())
                taskList.put(task.getTaskID().toString(), buildTaskProfile(task, startTime));
            for (LoggedTask task : job.getReduceTasks())
                taskList.put(task.getTaskID().toString(), buildTaskProfile(task, startTime));
            taskMap.put(jobId, taskList);

            jobList.add(profile);

            if (jobFinishTimeMS > simulationTime)
                simulationTime = jobFinishTimeMS;
        }
    }

    private JobProfile snapshot(JobProfile original) {
        JobProfile copy = Records.newRecord(JobProfile.class);
        copy.setId(original.getId());
        copy.setName(original.getName());
        copy.setUser(original.getUser() == null ? "default" : original.getUser());
        copy.setTotalMapTasks(original.getTotalMapTasks());
        copy.setTotalReduceTasks(original.getTotalReduceTasks());
        copy.setStartTime(original.getStartTime() > currentTime ? null : original.getStartTime());
        copy.setFinishTime(original.getFinishTime() > currentTime ? null : original.getFinishTime());
        //TODO copy all tasks with obfuscated times
        return copy;
    }

    @Override
    public <T extends GeneralDataEntity> T findById(DataEntityDB db, DataEntityType collection, String id) {
        if (DataEntityType.JOB.equals(collection)) {
            for (JobProfile job : jobList)
                if (job.getId().equals(id))
                    return (T) snapshot(job);
        }
        if (DataEntityType.TASK.equals(collection)) {
            JobId parent = Utils.parseTaskId(id).getJobId();
            return (T) taskMap.get(parent.toString()).get(id);
        }
        return null;

    }

    private void storeIfMoreRecent(JobProfile job, TreeMap<Long, JobProfile> list) {
        if (!list.isEmpty()) {
            if (list.firstKey() < job.getFinishTime())
                list.remove(list.firstKey());
            else
                return;
        }
        list.put(job.getFinishTime(), job);
    }

    public List<JobProfile> getComparableProfiles(DataEntityDB db, String user, int count) {
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

    public Map<String, List<String>> getFutureJobInfo() {
        Map<String, List<String>> ret = new HashMap<>(jobList.size());
        for (JobProfile job : jobList)
            if (job.getFinishTime() > currentTime) {
                List<String> tasks = new ArrayList<>(job.getTotalMapTasks() + job.getTotalReduceTasks());
                tasks.addAll(taskMap.get(job.getId()).keySet());
                ret.put(job.getId(), tasks);
            }
        return ret;
    }
}
