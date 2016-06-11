package org.apache.hadoop.tools.posum.database.client;

import org.apache.hadoop.tools.posum.common.records.dataentity.*;

import java.util.List;
import java.util.Map;

/**
 * Created by ane on 2/9/16.
 */
public interface DBInterface {

    /* Generic accessors */

    <T extends GeneralDataEntity> List<T> list(DataEntityType collection);

    <T extends GeneralDataEntity> T findById(DataEntityType collection, String id);

    <T extends GeneralDataEntity> List<T> find(DataEntityType collection, String field, Object value);

    <T extends GeneralDataEntity> List<T> find(DataEntityType collection, Map<String, Object> queryParams);

    /* Generic modifiers */

    <T extends GeneralDataEntity> String store(DataEntityType collection, T toInsert);

    //returns true if an existing object was overwritten
    <T extends GeneralDataEntity> boolean updateOrStore(DataEntityType apps, T toUpdate);

    void delete(DataEntityType collection, String id);

    void delete(DataEntityType collection, String field, Object value);

    void delete(DataEntityType collection, Map<String, Object> queryParams);

    /* Custom accessors */

    JobProfile getJobProfileForApp(String appId, String user);

    List<JobProfile> getComparableProfiles(String user, int count);

    JobConfProxy getJobConf(String jobId);
}

