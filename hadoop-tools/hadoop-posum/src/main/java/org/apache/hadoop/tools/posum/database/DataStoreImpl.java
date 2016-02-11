package org.apache.hadoop.tools.posum.database;

import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.tools.posum.database.records.JobProfile;
import org.apache.hadoop.tools.posum.database.records.TaskProfile;

import java.util.List;

/**
 * Created by ane on 2/9/16.
 */
public class DataStoreImpl implements DataStore{
    @Override
    public TaskProfile getTaskProfile(TaskID taskId) {
        return null;
    }

    @Override
    public JobProfile getJobProfile(JobID jobId) {
        return null;
    }

    @Override
    public List<JobProfile> getComparableProfiles(String user, int count) {
        return null;
    }
}
