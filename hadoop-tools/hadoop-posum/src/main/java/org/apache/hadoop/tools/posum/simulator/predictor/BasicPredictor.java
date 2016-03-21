package org.apache.hadoop.tools.posum.simulator.predictor;

import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.tools.posum.common.POSUMConfiguration;
import org.apache.hadoop.tools.posum.common.Utils;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityType;
import org.apache.hadoop.tools.posum.database.store.DataStore;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;

import java.util.List;

/**
 * Created by ane on 2/9/16.
 */
public class BasicPredictor extends JobBehaviorPredictor {

    public BasicPredictor(DataStore dataStore) {
        super(dataStore);
    }

    @Override
    public Integer predictJobDuration(String jobId) {
        Float duration = 0.0f;
        JobProfile current = dataStore.findById(DataEntityType.JOB, jobId);
        List<JobProfile> comparable = dataStore.getComparableProfiles(
                current.getUser(),
                conf.getInt(POSUMConfiguration.BUFFER,
                        POSUMConfiguration.BUFFER_DEFAULT)
        );
        if (comparable.size() < 1)
            return conf.getInt(POSUMConfiguration.AVERAGE_JOB_DURATION,
                    POSUMConfiguration.AVERAGE_JOB_DURATION_DEFAULT);
        for (JobProfile profile : comparable)
            duration += profile.getDuration();
        duration /= comparable.size();
        return duration.intValue();
    }

    @Override
    public Integer predictTaskDuration(String jobId, TaskType type) {
        Float duration = 0.0f;
        JobProfile current = dataStore.findById(DataEntityType.JOB, jobId);
        float currentAverage = TaskType.MAP.equals(type) ? current.getAvgMapDuration() : current.getAvgReduceDuration();
        if (currentAverage > 0)
            return new Float(currentAverage).intValue();

        List<JobProfile> comparable = dataStore.getComparableProfiles(
                current.getUser(),
                conf.getInt(POSUMConfiguration.BUFFER, POSUMConfiguration.BUFFER_DEFAULT)
        );
        if (comparable.size() < 1)
            return conf.getInt(POSUMConfiguration.AVERAGE_TASK_DURATION,
                    POSUMConfiguration.AVERAGE_TASK_DURATION_DEFAULT);
        for (JobProfile profile : comparable)
            duration += TaskType.MAP.equals(type) ? profile.getAvgMapDuration() : profile.getAvgReduceDuration();
        duration /= comparable.size();
        return duration.intValue();
    }

    @Override
    public Integer predictTaskDuration(String jobId, String taskId) {
        return predictTaskDuration(jobId, Utils.getTaskTypeFromId(taskId));
    }
}
