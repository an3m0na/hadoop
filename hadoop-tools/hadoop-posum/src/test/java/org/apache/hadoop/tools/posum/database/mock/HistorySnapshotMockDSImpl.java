package org.apache.hadoop.tools.posum.database.mock;

import org.apache.hadoop.tools.posum.common.records.dataentity.*;
import org.apache.hadoop.tools.posum.common.util.Utils;
import org.apache.hadoop.tools.posum.database.store.DataStoreImporter;
import org.apache.hadoop.yarn.util.Records;

/**
 * Created by ane on 7/26/16.
 */
public class HistorySnapshotMockDSImpl extends MockDataStoreImpl implements HistorySnapshotDS {

    private Long traceStart = 0L;
    private Long traceFinish = 0L;
    private Long currentOffset = 0L;
    private Long currentTime = 0L;
    private DataEntityDB mainDB = DataEntityDB.getMain();
    private DataEntityDB shadowDB = DataEntityDB.get(DataEntityDB.Type.MAIN, "shadow");

    public  HistorySnapshotMockDSImpl(String dataDumpFolderName){
        new DataStoreImporter(dataDumpFolderName).importTo(Utils.exposeDataStoreAsBroker(this));
    }

    private void recomputeSnapshot() {

//        while ((job = reader.getNext()) != null) {
//
//            String jobId = job.getJobID().toString();
//
//            long jobStartTimeMS = job.getSubmitTime();
//            long jobFinishTimeMS = job.getFinishTime();
//            if (startTime == 0) {
//                startTime = jobStartTimeMS;
//            }
//            jobStartTimeMS -= startTime;
//            jobFinishTimeMS -= startTime;
//            if (jobStartTimeMS < 0) {
//                jobFinishTimeMS = jobFinishTimeMS - jobStartTimeMS;
//                jobStartTimeMS = 0;
//            }
//
//            JobProfile profile = Records.newRecord(JobProfile.class);
//            profile.setId(jobId);
//            profile.setName(job.getJobName().getValue());
//            profile.setUser(job.getUser() == null ? "default" : job.getUser().getValue());
//            profile.setTotalMapTasks(job.getTotalMaps());
//            profile.setTotalReduceTasks(job.getTotalReduces());
//            profile.setStartTime(jobStartTimeMS);
//            profile.setFinishTime(jobFinishTimeMS);
//            //TODO continue with other job characteristics (look into computonsperbyte)
//
//            Map<String, TaskProfile> taskList = new HashMap<>(job.getMapTasks().size() + job.getReduceTasks().size());
//            for (LoggedTask task : job.getMapTasks())
//                taskList.put(task.getTaskID().toString(), buildTaskProfile(task, startTime));
//            for (LoggedTask task : job.getReduceTasks())
//                taskList.put(task.getTaskID().toString(), buildTaskProfile(task, startTime));
//            taskMap.put(jobId, taskList);
//
//            jobList.add(profile);
//
//            if (jobFinishTimeMS > simulationTime)
//                simulationTime = jobFinishTimeMS;
//        }
    }

    private JobProfile createJobSnapshot(JobProfile original) {
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
    public Long getSnapshotTime() {
        return currentTime;
    }

    @Override
    public void setSnapshotTime(Long time) {
        currentTime = time;
        currentOffset = time - traceStart;
        recomputeSnapshot();
    }

    @Override
    public Long getSnapshotOffset() {
        return currentOffset;
    }

    @Override
    public void setSnapshotOffset(Long offset) {
        currentTime = traceStart + offset;
        currentOffset = offset;
        recomputeSnapshot();
    }

    @Override
    public Long getTraceStartTime() {
        return traceStart;
    }

    @Override
    public Long getTraceFinishTime() {
        return traceFinish;
    }
}
