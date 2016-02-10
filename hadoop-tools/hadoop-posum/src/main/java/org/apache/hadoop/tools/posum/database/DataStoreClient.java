package org.apache.hadoop.tools.posum.database;

import org.apache.hadoop.tools.posum.predictor.JobProfile;
import org.apache.hadoop.tools.posum.predictor.TaskProfile;

import java.util.List;

/**
 * Created by ane on 2/9/16.
 */
public class DataStoreClient implements DataStore {
    public Float getAverageJobDuration(String user, String jobName) {
        return null;
    }

    @Override
    public TaskProfile getTaskProfile(String taskId) {
        return null;
    }

    @Override
    public JobProfile getJobProfile(String jobId) {
        return null;
    }

    @Override
    public List<JobProfile> getComparableProfiles(String user, int count) {
        return null;
    }
}
