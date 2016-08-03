package org.apache.hadoop.tools.posum.database.store;

import org.apache.hadoop.tools.posum.common.records.dataentity.*;
import org.apache.hadoop.tools.posum.database.client.DataBroker;

import java.util.List;
import java.util.Map;

/**
 * Created by ane on 2/9/16.
 */
public interface DataStore {

    <T extends GeneralDataEntity> T findById(DataEntityDB db, DataEntityCollection collection, String id);

    //TODO Make this have offset and limit
    List<String> listIds(DataEntityDB db, DataEntityCollection collection, Map<String, Object> queryParams);

    <T extends GeneralDataEntity> List<T> find(DataEntityDB db, DataEntityCollection collection, Map<String, Object> queryParams, int offsetOrZero, int limitOrZero);

    <T extends GeneralDataEntity> String store(DataEntityDB db, DataEntityCollection collection, T toInsert);

    //TODO turn this into a string that represents the upsertedId
    <T extends GeneralDataEntity> boolean updateOrStore(DataEntityDB db, DataEntityCollection apps, T toUpdate);

    void delete(DataEntityDB db, DataEntityCollection collection, String id);

    void delete(DataEntityDB db, DataEntityCollection collection, Map<String, Object> queryParams);

    DataBroker bindTo(DataEntityDB db);

    JobProfile getJobProfileForApp(DataEntityDB db, String appId, String user);

    void saveFlexFields(DataEntityDB db, String jobId, Map<String, String> newFields, boolean forHistory);

    void lockForRead(DataEntityDB db);

    void lockForWrite(DataEntityDB db);

    void unlockForRead(DataEntityDB db);

    void unlockForWrite(DataEntityDB db);

    Map<DataEntityDB, List<DataEntityCollection>> listExistingCollections();

    void clear();
}
