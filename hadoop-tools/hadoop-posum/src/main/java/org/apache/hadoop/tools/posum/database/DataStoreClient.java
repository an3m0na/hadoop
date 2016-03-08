package org.apache.hadoop.tools.posum.database;

import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.tools.posum.common.records.AppProfile;
import org.apache.hadoop.tools.posum.common.records.GeneralProfile;
import org.apache.hadoop.tools.posum.common.records.JobProfile;
import org.apache.hadoop.tools.posum.common.records.TaskProfile;
import org.apache.hadoop.yarn.api.records.ApplicationId;

import java.util.List;
import java.util.Map;

/**
 * Created by ane on 2/9/16.
 */
public class DataStoreClient implements DataStore {

    //TODO make it connect to actual database

    @Override
    public <T extends GeneralProfile> T findById(DataCollection collection, String id) {
        return null;
    }

    @Override
    public JobProfile getJobProfileForApp(String appId) {
        return null;
    }

    @Override
    public <T extends GeneralProfile> void store(DataCollection collection, T toInsert) {

    }

    @Override
    public List<JobProfile> getComparableProfiles(String user, int count) {
        return null;
    }

    @Override
    public <T extends GeneralProfile> boolean updateOrStore(DataCollection apps, T toUpdate) {
        return false;
    }

    @Override
    public void delete(DataCollection collection, String id) {

    }

    @Override
    public void delete(DataCollection collection, String field, Object value) {

    }

    @Override
    public void delete(DataCollection collection, Map<String, Object> queryParams) {

    }
}
