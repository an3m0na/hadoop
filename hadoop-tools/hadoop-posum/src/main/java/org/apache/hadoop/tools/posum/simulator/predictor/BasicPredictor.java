package org.apache.hadoop.tools.posum.simulator.predictor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.tools.posum.common.records.dataentity.TaskProfile;
import org.apache.hadoop.tools.posum.common.util.POSUMConfiguration;
import org.apache.hadoop.tools.posum.common.util.POSUMException;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityType;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;

import java.util.List;

/**
 * Created by ane on 2/9/16.
 */
public class BasicPredictor extends JobBehaviorPredictor {

    private static final Log logger = LogFactory.getLog(BasicPredictor.class);

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
                    DataEntityType.JOB_HISTORY,
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
        List<JobProfile> comparable = getComparableProfiles(job);
        if (comparable.size() < 1)
            return conf.getLong(POSUMConfiguration.AVERAGE_JOB_DURATION,
                    POSUMConfiguration.AVERAGE_JOB_DURATION_DEFAULT);
        Long duration = 0L;
        for (JobProfile profile : comparable)
            duration += profile.getDuration();
        duration /= comparable.size();
        return duration;
    }

    @Override
    public Long predictLocalMapTaskDuration(String jobId) {
        return predictTaskDuration(jobId, TaskType.MAP);
    }

    @Override
    public Long predictTaskDuration(String jobId, TaskType type) {
        JobProfile job = getDataStore().findById(DataEntityType.JOB, jobId);
        Long currentAverage = TaskType.MAP.equals(type) ? job.getAvgMapDuration() : job.getAvgReduceDuration();
        if (currentAverage > 0)
            return currentAverage;

        List<JobProfile> comparable = getComparableProfiles(job);
        if (comparable.size() < 1)
            return conf.getLong(POSUMConfiguration.AVERAGE_TASK_DURATION,
                    POSUMConfiguration.AVERAGE_TASK_DURATION_DEFAULT);
        Long duration = 0L;
        for (JobProfile profile : comparable)
            duration += TaskType.MAP.equals(type) ? profile.getAvgMapDuration() : profile.getAvgReduceDuration();
        duration /= comparable.size();
        return duration;
    }

    @Override
    public Long predictTaskDuration(String taskId) {
        TaskProfile task = getDataStore().findById(DataEntityType.TASK, taskId);
        if (task == null)
            return null;
        return predictTaskDuration(task.getJobId(), task.getType());
    }
}
