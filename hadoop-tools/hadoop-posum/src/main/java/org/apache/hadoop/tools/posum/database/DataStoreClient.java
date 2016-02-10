package org.apache.hadoop.tools.posum.database;

import org.apache.hadoop.tools.posum.database.records.JobProfile;
import org.apache.hadoop.tools.posum.database.records.TaskProfile;

import java.util.List;

/**
 * Created by ane on 2/9/16.
 */
public class DataStoreClient implements DataStore {

    //TODO make it connect to actual database

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
