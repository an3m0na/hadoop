package org.apache.hadoop.tools.posum.common.records.dataentity;

import org.apache.hadoop.mapreduce.v2.api.records.JobState;

/**
 * Created by ane on 2/8/16.
 */
public class JobProfile extends GeneralDataEntity {
    private String name;
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
    private Integer avgMapDuration;
    private Integer avgReduceDuration;
    private Integer avgTaskDuration;
    private Integer avgShuffleDuration;
    private Integer avgMergeDuration;

    //NOT INCLUDED:
//    <failedReduceAttempts>0</failedReduceAttempts>
//    <killedReduceAttempts>0</killedReduceAttempts>
//    <successfulReduceAttempts>1</successfulReduceAttempts>
//    <failedMapAttempts>0</failedMapAttempts>
//    <killedMapAttempts>0</killedMapAttempts>
//    <successfulMapAttempts>30</successfulMapAttempts>

    public JobProfile() {
    }

    public JobProfile(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
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
        if (finishTime == 0) return -1;
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

    public Long getAvgSplitSize() {
        if (inputSplits != null && inputSplits != 0)
            return inputBytes / inputSplits;
        return -1L;
    }

    public Integer getAvgMapDuration() {
        return avgMapDuration;
    }

    public void setAvgMapDuration(Integer avgMapDuration) {
        this.avgMapDuration = avgMapDuration;
    }

    public Integer getAvgReduceDuration() {
        return avgReduceDuration;
    }

    public void setAvgReduceDuration(Integer avgReduceDuration) {
        this.avgReduceDuration = avgReduceDuration;
    }

    public Integer getAvgTaskDuration() {
        return avgTaskDuration;
    }

    public void setAvgTaskDuration(Integer avgTaskDuration) {
        this.avgTaskDuration = avgTaskDuration;
    }

    //    private float computeAverageTaskDuration() {
//        Pair<Float, Integer> maps = accumulateTasks(mapTasks);
//        Pair<Float, Integer> reds = accumulateTasks(reduceTasks);
//        if (maps.getSecond() == 0)
//            return maps.getFirst();
//        if (reds.getSecond() == 0)
//            return reds.getFirst();
//        return (maps.getFirst() * maps.getSecond() + reds.getFirst() * reds.getSecond()) /
//                (maps.getSecond() + reds.getSecond());
//    }
//
//    private Pair<Float, Integer> accumulateTasks(Map<String, TaskProfile> tasks) {
//        float duration = 0.0f;
//        int size = 0;
//        for (TaskProfile t : tasks.values()) {
//            if (t.getDuration() != null) {
//                duration += t.getDuration();
//                size++;
//            }
//        }
//        return new Pair<>(duration / size, size);
//    }
//
//    public float computeAverageTaskDuration(TaskType type) {
//        switch (type) {
//            case MAP:
//                return accumulateTasks(mapTasks).getFirst();
//            case REDUCE:
//                return accumulateTasks(reduceTasks).getFirst();
//            default:
//                return computeAverageTaskDuration();
//        }
//    }
//
//    public HashMap<String, TaskProfile> getMapTasks() {
//        return mapTasks;
//    }
//
//    public HashMap<String, TaskProfile> getReduceTasks() {
//        return reduceTasks;
//    }

    public void populate(String jobName,
                         String user,
                         Integer totalMaps,
                         Integer totalReduces,
                         Long startTime,
                         Long finishTime) {

        setName(jobName);
        setUser(user);
        setTotalMapTasks(totalMaps);
        setTotalReduceTasks(totalReduces);
        setStartTime(startTime);
        setFinishTime(finishTime);
    }

    @Override
    public String toString() {
        return "JobProfile[" + getId() + "]{" +
                "jobName='" + name + '\'' +
                ", appId='" + appId + '\'' +
                ", user='" + user + '\'' +
                ", totalMapTasks=" + totalMapTasks +
                ", totalReduceTasks=" + totalReduceTasks +
                ", inputSplits=" + inputSplits +
                ", inputBytes=" + inputBytes +
                ", outputBytes=" + outputBytes +
                ", submitTime=" + submitTime +
                ", startTime=" + startTime +
                ", finishTime=" + finishTime +
                ", state=" + state +
                ", mapProgress=" + mapProgress +
                ", reduceProgress=" + reduceProgress +
                ", completedMaps=" + completedMaps +
                ", completedReduces=" + completedReduces +
                ", uberized=" + uberized +
                ", avgMapDuration=" + avgMapDuration +
                ", avgReduceDuration=" + avgReduceDuration +
                ", avgTaskDuration=" + avgTaskDuration +
                '}';
    }

    public void setAvgShuffleDuration(Integer avgShuffleDuration) {
        this.avgShuffleDuration = avgShuffleDuration;
    }

    public Integer getAvgShuffleDuration() {
        return avgShuffleDuration;
    }

    public void setAvgMergeDuration(Integer avgMergeDuration) {
        this.avgMergeDuration = avgMergeDuration;
    }

    public Integer getAvgMergeDuration() {
        return avgMergeDuration;
    }
}
