package org.apache.hadoop.tools.posum.database;

import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.tools.posum.common.records.JobProfile;
import org.apache.hadoop.tools.posum.common.records.TaskProfile;

import java.util.List;

/**
 * Created by ane on 2/9/16.
 */
public class DataStoreImpl implements DataStore{
    @Override
    public TaskProfile getTaskProfile(TaskId taskId) {
        return null;
    }

    @Override
    public JobProfile getJobProfile(JobId jobId) {
        return null;
    }

    @Override
    public List<JobProfile> getComparableProfiles(String user, int count) {
        return null;
    }
}
