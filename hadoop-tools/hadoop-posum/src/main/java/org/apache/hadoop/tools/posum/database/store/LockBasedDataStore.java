package org.apache.hadoop.tools.posum.database.store;

import org.apache.hadoop.tools.posum.common.records.dataentity.*;
import org.apache.hadoop.tools.posum.database.client.Database;

import java.util.List;
import java.util.Map;

/**
 * Created by ane on 2/9/16.
 */
public interface LockBasedDataStore {

    <T extends GeneralDataEntity> T findById(DataEntityDB db, DataEntityCollection collection, String id);

    <T extends GeneralDataEntity> List<T> find(DataEntityDB db,
                                               DataEntityCollection collection,
                                               Map<String, Object> params,
                                               String sortField,
                                               boolean sortDescending,
                                               int offsetOrZero,
                                               int limitOrZero);

    List<String> findIds(DataEntityDB db,
                         DataEntityCollection collection,
                         Map<String, Object> queryParams,
                         String sortField,
                         boolean sortDescending,
                         int offsetOrZero,
                         int limitOrZero);

    <T extends GeneralDataEntity> String store(DataEntityDB db, DataEntityCollection collection, T toInsert);

    <T extends GeneralDataEntity> String updateOrStore(DataEntityDB db, DataEntityCollection apps, T toUpdate);

    void delete(DataEntityDB db, DataEntityCollection collection, String id);

    void delete(DataEntityDB db, DataEntityCollection collection, Map<String, Object> queryParams);

    Database bindTo(DataEntityDB db);

    void lockForRead(DataEntityDB db);

    void lockForWrite(DataEntityDB db);

    void unlockForRead(DataEntityDB db);

    void unlockForWrite(DataEntityDB db);

    void lockAll();

    void unlockAll();

    Map<DataEntityDB, List<DataEntityCollection>> listExistingCollections();

    void clear();
}
