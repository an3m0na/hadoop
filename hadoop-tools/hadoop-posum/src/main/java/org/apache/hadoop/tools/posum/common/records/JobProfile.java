package org.apache.hadoop.tools.posum.common.records;

import org.apache.commons.math3.util.Pair;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;

/**
 * Created by ane on 2/8/16.
 */
public class JobProfile extends GeneralProfile {
    private String jobName;
    private String appId;
    private String user;
    private Integer totalMapTasks;
    private Integer totalReduceTasks;
    private Integer inputSplits; // only from conf
    private Long inputBytes; // only from conf
    private Long outputBytes; // only in history
    private Long submitTime; // only in history
    private Long startTime = 0L;
    private Long finishTime = 0L;
    private JobState state;
    private Float mapProgress;
    private Float reduceProgress;
    private Integer completedMaps;
    private Integer completedReduces;
    private Boolean uberized;

    private HashMap<String, TaskProfile> mapTasks = new HashMap<>();
    private HashMap<String, TaskProfile> reduceTasks = new HashMap<>();

    private ReadWriteLock lock = new ReentrantReadWriteLock();


    public JobProfile(String id) {
        this.id = id;
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

    public void setFinishTime(Long finishTime) {
        this.finishTime = finishTime;
    }

    public Integer getDuration() {
        if(finishTime == 0) return -1;
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

    public String getAppId() {
        return appId;
    }

    public void setAppId(String appId) {
        this.appId = appId;
    }

    public JobState getState() {
        return state;
    }

    public void setState(String state) {
        if (state != null)
            this.state = JobState.valueOf(state);
    }

    public Float getMapProgress() {
        return mapProgress;
    }

    public void setMapProgress(Float mapProgress) {
        this.mapProgress = mapProgress;
    }

    public Float getReduceProgress() {
        return reduceProgress;
    }

    public void setReduceProgress(Float reduceProgress) {
        this.reduceProgress = reduceProgress;
    }

    public Integer getCompletedMaps() {
        return completedMaps;
    }

    public void setCompletedMaps(Integer completedMaps) {
        this.completedMaps = completedMaps;
    }

    public Integer getCompletedReduces() {
        return completedReduces;
    }

    public void setCompletedReduces(Integer completedReduces) {
        this.completedReduces = completedReduces;
    }

    public Boolean isUberized() {
        return uberized;
    }

    public void setUberized(Boolean uberized) {
        this.uberized = uberized;
    }

    public Integer getInputSplits() {
        return inputSplits;
    }

    public void setInputSplits(Integer inputSplits) {
        this.inputSplits = inputSplits;
    }

    public Long getInputBytes() {
        return inputBytes;
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
            mapTasks.put(task.getId(), task);
        }
        if (task.getType().equals(TaskType.REDUCE)) {
            reduceTasks.put(task.getId(), task);
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
                         Integer totalMaps,
                         Integer totalReduces,
                         Long startTime,
                         Long finishTime) {

        setJobName(jobName);
        setUser(user);
        setTotalMapTasks(totalMaps);
        setTotalReduceTasks(totalReduces);
        setStartTime(startTime);
        setFinishTime(finishTime);
    }

    @Override
    public String toString() {
        return "JobProfile{" +
                "id=" + id +
                ", jobName='" + jobName + '\'' +
                ", user='" + user + '\'' +
                ", totalMapTasks=" + totalMapTasks +
                ", totalReduceTasks=" + totalReduceTasks +
                ", inputBytes=" + inputBytes +
                ", outputBytes=" + outputBytes +
                ", submitTime=" + submitTime +
                ", startTime=" + startTime +
                ", finishTime=" + finishTime +
                ", mapTasks=" + mapTasks +
                ", reduceTasks=" + reduceTasks +
                ", lock=" + lock +
                '}';
    }
}
