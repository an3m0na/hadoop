package org.apache.hadoop.tools.posum.database.store;

import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityType;
import org.apache.hadoop.tools.posum.common.records.dataentity.GeneralDataEntity;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;

import java.util.List;
import java.util.Map;

/**
 * Created by ane on 2/9/16.
 */
public interface DataStore {


    <T extends GeneralDataEntity> T findById(DataEntityType collection, String id);

    JobProfile getJobProfileForApp(String appId);

    <T extends GeneralDataEntity> void store(DataEntityType collection, T toInsert);

    List<JobProfile> getComparableProfiles(String user, int count);

    //returns true if an existing object was overwritten
    <T extends GeneralDataEntity> boolean updateOrStore(DataEntityType apps, T toUpdate);

    void delete(DataEntityType collection, String id);

    void delete(DataEntityType collection, String field, Object value);

    void delete(DataEntityType collection, Map<String, Object> queryParams);
}
