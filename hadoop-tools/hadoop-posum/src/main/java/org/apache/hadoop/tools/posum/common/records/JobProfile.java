package org.apache.hadoop.tools.posum.common.records;

import org.apache.commons.math3.util.Pair;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;

/**
 * Created by ane on 2/8/16.
 */
public class JobProfile {
    private String jobId;
    private String jobName;
    private String user;
    private String queue;
    private Integer totalMapTasks;
    private Integer totalReduceTasks;

    private Long inputBytes;
    private Long outputBytes;
    private Long submitTime;
    private Long startTime;
    private Long finishTime;
    private Float reportedProgress;

    private HashMap<String, TaskProfile> mapTasks = new HashMap<>();
    private HashMap<String, TaskProfile> reduceTasks = new HashMap<>();

    private ReadWriteLock lock = new ReentrantReadWriteLock();


    public JobProfile(String jobId, Long submitTime) {
        this.jobId = jobId;
        this.submitTime = submitTime;
    }

    public String getJobId() {
        return jobId;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public Long getStartTime() {
        return startTime;
    }

    public void setStartTime(Long startTime) {
        this.startTime = startTime;
    }

    public Long getFinishTime() {
        return finishTime;
    }

    public Float getReportedProgress() {
        return reportedProgress;
    }

    public void setFinishTime(Long finishTime) {
        this.finishTime = finishTime;
    }

    public void setReportedProgress(Float reportedProgress) {
        this.reportedProgress = reportedProgress;
    }

    public Integer getDuration() {
        return new Long(finishTime - startTime).intValue();
    }

    public void setTotalMapTasks(Integer totalMapTasks) {
        this.totalMapTasks = totalMapTasks;
    }

    public void setTotalReduceTasks(Integer totalReduceTasks) {
        this.totalReduceTasks = totalReduceTasks;
    }

    public void setInputBytes(Long inputBytes) {
        this.inputBytes = inputBytes;
    }

    public Long getOutputBytes() {
        return outputBytes;
    }

    public void setOutputBytes(Long outputBytes) {
        this.outputBytes = outputBytes;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }

    public Long getSubmitTime() {
        return submitTime;
    }

    public void setSubmitTime(Long submitTime) {
        this.submitTime = submitTime;
    }

    public Integer getTotalMapTasks() {
        return totalMapTasks;
    }

    public Integer getTotalReduceTasks() {
        return totalReduceTasks;
    }

    private float computeAverageTaskDuration() {
        lock.readLock().lock();
        Pair<Float, Integer> maps = accumulateTasks(mapTasks);
        Pair<Float, Integer> reds = accumulateTasks(reduceTasks);
        lock.readLock().unlock();
        if (maps.getSecond() == 0)
            return maps.getFirst();
        if (reds.getSecond() == 0)
            return reds.getFirst();
        return (maps.getFirst() * maps.getSecond() + reds.getFirst() * reds.getSecond()) /
                (maps.getSecond() + reds.getSecond());
    }

    private Pair<Float, Integer> accumulateTasks(Map<String, TaskProfile> tasks) {
        lock.readLock().lock();
        float duration = 0.0f;
        int size = 0;
        for (TaskProfile t : tasks.values()) {
            if (t.getDuration() != null) {
                duration += t.getDuration();
                size++;
            }
        }
        lock.readLock().lock();
        return new Pair<>(duration / size, size);
    }

    public float computeAverageTaskDuration(TaskType type) {
        switch (type) {
            case MAP:
                return accumulateTasks(mapTasks).getFirst();
            case REDUCE:
                return accumulateTasks(reduceTasks).getFirst();
            default:
                return computeAverageTaskDuration();
        }
    }

    public void recordTask(TaskProfile task) {
        lock.writeLock().lock();
        if (task.getType().equals(TaskType.MAP)) {
            mapTasks.put(task.getTaskId(), task);
        }
        if (task.getType().equals(TaskType.REDUCE)) {
            reduceTasks.put(task.getTaskId(), task);
        }
        lock.writeLock().unlock();
    }

    public TaskProfile getTask(TaskId taskId) {
        switch (taskId.getTaskType()) {
            case MAP:
                return mapTasks.get(taskId);
            case REDUCE:
                return reduceTasks.get(taskId);
            default:
                return null;
        }
    }

    public HashMap<String, TaskProfile> getMapTasks() {
        return mapTasks;
    }

    public HashMap<String, TaskProfile> getReduceTasks() {
        return reduceTasks;
    }

    public void populate(String jobName,
                         String user,
                         String queue,
                         Integer totalMaps,
                         Integer totalReduces,
                         Long startTime,
                         Long finishTime) {

        setJobName(jobName);
        setUser(user);
        setQueue(queue);
        setTotalMapTasks(totalMaps);
        setTotalReduceTasks(totalReduces);
        setStartTime(startTime);
        setFinishTime(finishTime);
    }

    @Override
    public String toString() {
        return "JobProfile{" +
                "jobId=" + jobId +
                ", jobName='" + jobName + '\'' +
                ", user='" + user + '\'' +
                ", queue='" + queue + '\'' +
                ", totalMapTasks=" + totalMapTasks +
                ", totalReduceTasks=" + totalReduceTasks +
                ", inputBytes=" + inputBytes +
                ", outputBytes=" + outputBytes +
                ", submitTime=" + submitTime +
                ", startTime=" + startTime +
                ", finishTime=" + finishTime +
                ", reportedProgress=" + reportedProgress +
                ", mapTasks=" + mapTasks +
                ", reduceTasks=" + reduceTasks +
                ", lock=" + lock +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        JobProfile that = (JobProfile) o;

        return jobId.equals(that.jobId);

    }

    @Override
    public int hashCode() {
        return jobId.hashCode();
    }
}
