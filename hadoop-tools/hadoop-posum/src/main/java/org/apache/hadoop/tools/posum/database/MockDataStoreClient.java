package org.apache.hadoop.tools.posum.database;

import org.apache.hadoop.tools.posum.predictor.JobProfile;
import org.apache.hadoop.tools.posum.predictor.TaskProfile;

import java.util.List;

/**
 * Created by ane on 2/10/16.
 */
public class MockDataStoreClient extends DataStoreClient {
    @Override
    public Float getAverageJobDuration(String user, String jobName) {
        return 60000f;
    }

    @Override
    public TaskProfile getTaskProfile(String taskId) {
        return super.getTaskProfile(taskId);
    }

    @Override
    public JobProfile getJobProfile(String jobId) {
        return super.getJobProfile(jobId);
    }

    @Override
    public List<JobProfile> getComparableProfiles(String user, int count) {
        return super.getComparableProfiles(user, count);
    }
}
