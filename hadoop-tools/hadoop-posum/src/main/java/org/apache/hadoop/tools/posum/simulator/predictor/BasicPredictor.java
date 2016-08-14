package org.apache.hadoop.tools.posum.simulator.predictor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.tools.posum.common.records.call.FindByIdCall;
import org.apache.hadoop.tools.posum.common.records.call.FindByQueryCall;
import org.apache.hadoop.tools.posum.common.records.call.query.QueryUtils;
import org.apache.hadoop.tools.posum.common.util.PosumConfiguration;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;

import java.util.List;

/**
 * Created by ane on 2/9/16.
 */
public class BasicPredictor extends JobBehaviorPredictor {

    private static final Log logger = LogFactory.getLog(BasicPredictor.class);

    private List<JobProfile> getComparableProfiles(JobProfile job) {
        int bufferLimit = conf.getInt(PosumConfiguration.PREDICTION_BUFFER,
                PosumConfiguration.PREDICTION_BUFFER_DEFAULT);
        // get past jobs with the same name
        FindByQueryCall getComparableJobs = FindByQueryCall.newInstance(
                DataEntityCollection.JOB_HISTORY,
                QueryUtils.is("name", job.getName()),
                -bufferLimit,
                bufferLimit
        );
        List<JobProfile> comparable = getDatabase().executeDatabaseCall(getComparableJobs).getEntities();
        if (comparable.size() < 1) {
            // get past jobs at least by the same user
            getComparableJobs.setQuery(QueryUtils.is("user", job.getUser()));
            comparable = getDatabase().executeDatabaseCall(getComparableJobs).getEntities();
        }
        return comparable;
    }

    @Override
    public Long predictJobDuration(String jobId) {
        FindByIdCall getJob = FindByIdCall.newInstance(DataEntityCollection.JOB, jobId);
        JobProfile job = getDatabase().executeDatabaseCall(getJob).getEntity();
        List<JobProfile> comparable = getComparableProfiles(job);
        if (comparable.size() < 1)
            return conf.getLong(PosumConfiguration.AVERAGE_JOB_DURATION,
                    PosumConfiguration.AVERAGE_JOB_DURATION_DEFAULT);
        Long duration = 0L;
        for (JobProfile profile : comparable)
            duration += profile.getDuration();
        duration /= comparable.size();
        return duration;
    }

    @Override
    public Long predictTaskDuration(String jobId, TaskType type) {
        FindByIdCall getJob = FindByIdCall.newInstance(DataEntityCollection.JOB, jobId);
        JobProfile job = getDatabase().executeDatabaseCall(getJob).getEntity();
        Long currentAverage = TaskType.MAP.equals(type) ? job.getAvgMapDuration() : job.getAvgReduceDuration();
        if (currentAverage > 0)
            return currentAverage;

        List<JobProfile> comparable = getComparableProfiles(job);
        if (comparable.size() < 1)
            return conf.getLong(PosumConfiguration.AVERAGE_TASK_DURATION,
                    PosumConfiguration.AVERAGE_TASK_DURATION_DEFAULT);
        Long duration = 0L;
        for (JobProfile profile : comparable)
            duration += TaskType.MAP.equals(type) ? profile.getAvgMapDuration() : profile.getAvgReduceDuration();
        duration /= comparable.size();
        return duration;
    }
}
