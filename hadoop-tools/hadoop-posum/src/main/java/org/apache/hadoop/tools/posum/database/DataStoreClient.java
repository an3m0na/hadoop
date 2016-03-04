package org.apache.hadoop.tools.posum.database;

import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.tools.posum.common.records.AppProfile;
import org.apache.hadoop.tools.posum.common.records.JobProfile;
import org.apache.hadoop.tools.posum.common.records.TaskProfile;
import org.apache.hadoop.yarn.api.records.ApplicationId;

import java.util.List;

/**
 * Created by ane on 2/9/16.
 */
public class DataStoreClient implements DataStore {

    //TODO make it connect to actual database

    @Override
    public TaskProfile getTaskProfile(TaskId taskId) {
        return null;
    }

    @Override
    public JobProfile getJobProfile(String jobId) {
        return null;
    }

    @Override
    public AppProfile getAppProfile(ApplicationId appId) {
        return null;
    }

    @Override
    public <T> void store(DataCollection collection, T toInsert) {

    }

    @Override
    public List<JobProfile> getComparableProfiles(String user, int count) {
        return null;
    }
}
