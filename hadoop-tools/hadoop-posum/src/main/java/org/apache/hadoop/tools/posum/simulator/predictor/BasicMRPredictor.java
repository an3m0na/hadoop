package org.apache.hadoop.tools.posum.simulator.predictor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityType;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.util.POSUMConfiguration;
import org.apache.hadoop.tools.posum.common.util.Utils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by ane on 2/9/16.
 */
public class BasicMRPredictor extends JobBehaviorPredictor {

    public BasicMRPredictor(Configuration conf) {
        super(conf);
    }

    @Override
    public void preparePredictor() {

    }

    private List<JobProfile> getComparableProfiles(JobProfile job) {
        // get past jobs with the same name
        List<JobProfile> comparable = getDataStore().find(
                DataEntityType.JOB_HISTORY,
                "name",
                job.getName(),
                0,
                conf.getInt(POSUMConfiguration.PREDICTION_BUFFER,
                        POSUMConfiguration.PREDICTION_BUFFER_DEFAULT)
        );
        if (comparable.size() < 1) {
            // get past jobs at least by the same user
            comparable = getDataStore().find(
                    DataEntityType.JOB,
                    "user",
                    job.getUser(),
                    0,
                    conf.getInt(POSUMConfiguration.PREDICTION_BUFFER,
                            POSUMConfiguration.PREDICTION_BUFFER_DEFAULT)
            );
        }
        return comparable;
    }

    @Override
    public Long predictJobDuration(String jobId) {
        JobProfile job = getDataStore().findById(DataEntityType.JOB, jobId);
        Map<TaskType, Long> taskDurations = predictTaskDurations(job);
        return taskDurations.get(TaskType.MAP) * job.getTotalMapTasks() +
                taskDurations.get(TaskType.REDUCE) * job.getTotalReduceTasks();
    }

    private Map<TaskType, Long> predictTaskDurations(JobProfile job) {
        Map<TaskType, Long> durations = new HashMap<>(2);
        durations.put(TaskType.MAP, job.getAvgMapDuration());
        durations.put(TaskType.REDUCE, job.getAvgReduceDuration());

        if (durations.get(TaskType.MAP) == 0) {
            List<JobProfile> comparable = getComparableProfiles(job);
            if (comparable.size() < 1) {
                Long defaultDuration = conf.getLong(POSUMConfiguration.AVERAGE_JOB_DURATION,
                        POSUMConfiguration.AVERAGE_JOB_DURATION_DEFAULT);
                durations.put(TaskType.MAP, defaultDuration);
                durations.put(TaskType.REDUCE, defaultDuration);
                return durations;
            }

            long mapTime = 0, reduceTime = 0;
            for (JobProfile profile : comparable) {
                mapTime += profile.getAvgMapDuration();
                double inputScale = 1.0;
                if (job.getTotalInputBytes() != null &&
                        profile.getTotalInputBytes() != null &&
                        profile.getTotalInputBytes() != 0)
                    inputScale = job.getTotalInputBytes() / profile.getTotalInputBytes();
                reduceTime += profile.getAvgReduceDuration() * inputScale;
            }
            durations.put(TaskType.MAP, mapTime / comparable.size());
            durations.put(TaskType.REDUCE, reduceTime / comparable.size());
        }
        return durations;
    }

    @Override
    public Long predictLocalMapTaskDuration(String jobId) {
        return predictTaskDuration(jobId, TaskType.MAP);
    }

    @Override
    public Long predictTaskDuration(String jobId, TaskType type) {
        JobProfile job = getDataStore().findById(DataEntityType.JOB, jobId);
        Map<TaskType, Long> taskDurations = predictTaskDurations(job);
        return taskDurations.get(type);
    }

    @Override
    public Long predictTaskDuration(String jobId, String taskId) {
        return predictTaskDuration(jobId, Utils.getTaskTypeFromId(taskId));
    }
}
