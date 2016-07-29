package org.apache.hadoop.tools.posum.database.client;

import org.apache.hadoop.tools.posum.common.records.dataentity.*;

import java.util.List;
import java.util.Map;

/**
 * Created by ane on 2/9/16.
 */
public interface DBInterface {

    /* Generic accessors */

    <T extends GeneralDataEntity> List<T> list(DataEntityCollection collection);

    List<String> listIds(DataEntityCollection collection, Map<String, Object> queryParams);

    <T extends GeneralDataEntity> T findById(DataEntityCollection collection, String id);

    <T extends GeneralDataEntity> List<T> find(DataEntityCollection collection, String field, Object value);

    <T extends GeneralDataEntity> List<T> find(DataEntityCollection collection, String field, Object value, int offsetOrZero, int limitOrZero);

    <T extends GeneralDataEntity> List<T> find(DataEntityCollection collection, Map<String, Object> queryParams);

    <T extends GeneralDataEntity> List<T> find(DataEntityCollection collection, Map<String, Object> queryParams, int offsetOrZero, int limitOrZero);

    /* Generic modifiers */

    <T extends GeneralDataEntity> String store(DataEntityCollection collection, T toInsert);

    //returns true if an existing object was overwritten
    <T extends GeneralDataEntity> boolean updateOrStore(DataEntityCollection collection, T toUpdate);

    void delete(DataEntityCollection collection, String id);

    void delete(DataEntityCollection collection, String field, Object value);

    void delete(DataEntityCollection collection, Map<String, Object> queryParams);

    /* Custom accessors */

    JobProfile getJobProfileForApp(String appId, String user);

    JobConfProxy getJobConf(String jobId);

    void saveFlexFields(String jobId, Map<String, String> newFields, boolean forHistory);
}

