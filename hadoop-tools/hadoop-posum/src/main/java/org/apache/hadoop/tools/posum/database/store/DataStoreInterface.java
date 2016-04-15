package org.apache.hadoop.tools.posum.database.store;

import org.apache.hadoop.tools.posum.common.util.POSUMException;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityType;
import org.apache.hadoop.tools.posum.common.records.dataentity.GeneralDataEntity;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;

import java.util.List;
import java.util.Map;

/**
 * Created by ane on 2/9/16.
 */
public interface DataStoreInterface {

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

    JobProfile getJobProfileForApp(String appId);

    List<JobProfile> getComparableProfiles(String user, int count);

    void runTransaction(DataTransaction transaction) throws POSUMException;


}
