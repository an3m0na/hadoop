package org.apache.hadoop.tools.posum.predictor;

import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.mapreduce.TaskType;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by ane on 2/8/16.
 */
public class JobProfile {
    private String jobId;
    private String jobName;
    private String user;
    private Integer totalMapTasks;
    private Integer totalReduceTasks;

    private Long inputBytes;
    private Long outputBytes;
    private Long startTime;
    private Long finishTime;
    private Float reportedProgress;

    private ArrayList<TaskProfile> mapTasks = new ArrayList<>();
    private ArrayList<TaskProfile> reduceTasks = new ArrayList<>();

    private ReadWriteLock lock = new ReentrantReadWriteLock();


    JobProfile(String jobId, Long startTime) {
        this.jobId = jobId;
        this.startTime = startTime;
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

    public long getInputBytes() {
        return inputBytes;
    }

    public void setInputBytes(long inputBytes) {
        this.inputBytes = inputBytes;
    }

    public int getTotalMapTasks() {
        return totalMapTasks;
    }

    public void setTotalMapTasks(int totalMapTasks) {
        this.totalMapTasks = totalMapTasks;
    }

    public int getTotalReduceTasks() {
        return totalReduceTasks;
    }

    public void setTotalReduceTasks(int totalReduceTasks) {
        this.totalReduceTasks = totalReduceTasks;
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

    private Pair<Float, Integer> accumulateTasks(List<TaskProfile> tasks) {
        lock.readLock().lock();
        float duration = 0.0f;
        int size = 0;
        for (TaskProfile t : tasks) {
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
        }
        return 0f;
    }

    public void recordTask(TaskProfile task) {
        lock.writeLock().lock();
        if (task.getType().equals(TaskType.MAP)) {
            mapTasks.add(task);
        }
        if (task.getType().equals(TaskType.REDUCE)) {
            reduceTasks.add(task);
        }
        lock.writeLock().unlock();
    }
}
