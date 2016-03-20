package org.apache.hadoop.tools.posum.database;

import org.apache.hadoop.tools.posum.common.records.profile.GeneralProfile;
import org.apache.hadoop.tools.posum.common.records.profile.JobProfile;

import java.util.List;
import java.util.Map;

/**
 * Created by ane on 2/9/16.
 */
public interface DataStore {


    <T extends GeneralProfile> T findById(DataCollection collection, String id);

    JobProfile getJobProfileForApp(String appId);

    <T extends GeneralProfile> void store(DataCollection collection, T toInsert);

    List<JobProfile> getComparableProfiles(String user, int count);

    //returns true if an existing object was overwritten
    <T extends GeneralProfile> boolean updateOrStore(DataCollection apps, T toUpdate);

    void delete(DataCollection collection, String id);

    void delete(DataCollection collection, String field, Object value);

    void delete(DataCollection collection, Map<String, Object> queryParams);
}
