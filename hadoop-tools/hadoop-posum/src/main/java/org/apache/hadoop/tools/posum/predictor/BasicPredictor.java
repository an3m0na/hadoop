package org.apache.hadoop.tools.posum.predictor;

import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.tools.posum.POSUMConfiguration;
import org.apache.hadoop.tools.posum.database.DataStore;
import org.apache.hadoop.tools.posum.database.records.JobProfile;
import org.apache.hadoop.tools.posum.database.records.TaskProfile;

import java.util.List;

/**
 * Created by ane on 2/9/16.
 */
public class BasicPredictor extends JobBehaviorPredictor {

    public BasicPredictor(DataStore dataStore) {
        super(dataStore);
    }

    @Override
    public Integer predictJobDuration(JobID jobId) {
        Float duration = 0.0f;
        JobProfile current = dataStore.getJobProfile(jobId);
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
    public Integer predictTaskDuration(JobID jobId, TaskType type) {
        Float duration = 0.0f;
        JobProfile current = dataStore.getJobProfile(jobId);
        float currentAverage = current.computeAverageTaskDuration(type);
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
            duration += profile.computeAverageTaskDuration(type);
        duration /= comparable.size();
        return duration.intValue();
    }

    @Override
    public Integer predictTaskDuration(JobID jobId, TaskID taskId) {
        return predictTaskDuration(jobId, taskId.getTaskType());
    }
}
