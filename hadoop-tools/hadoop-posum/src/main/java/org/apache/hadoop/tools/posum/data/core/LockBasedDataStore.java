package org.apache.hadoop.tools.posum.data.core;

import org.apache.hadoop.tools.posum.client.data.DataStore;
import org.apache.hadoop.tools.posum.common.records.call.query.DatabaseQuery;
import org.apache.hadoop.tools.posum.common.records.dataentity.*;

import java.util.List;
import java.util.Map;

public interface LockBasedDataStore extends DataStore{

    <T extends GeneralDataEntity<T>> T findById(DataEntityDB db, DataEntityCollection collection, String id);

    <T extends GeneralDataEntity<T>> List<T> find(DataEntityDB db,
                                               DataEntityCollection collection,
                                               DatabaseQuery query,
                                               String sortField,
                                               boolean sortDescending,
                                               int offsetOrZero,
                                               int limitOrZero);

    List<String> findIds(DataEntityDB db,
                         DataEntityCollection collection,
                         DatabaseQuery query,
                         String sortField,
                         boolean sortDescending,
                         int offsetOrZero,
                         int limitOrZero);

    <T extends GeneralDataEntity<T>> String store(DataEntityDB db, DataEntityCollection collection, T toStore);

    <T extends GeneralDataEntity<T>> void storeAll(DataEntityDB db, DataEntityCollection collection, List<T> toStore);

    <T extends GeneralDataEntity<T>> String updateOrStore(DataEntityDB db, DataEntityCollection apps, T toUpdate);

    void delete(DataEntityDB db, DataEntityCollection collection, String id);

    void delete(DataEntityDB db, DataEntityCollection collection, DatabaseQuery query);

    String getRawDocuments(DataEntityDB db, DataEntityCollection collection, DatabaseQuery query);

    void lockForRead(DataEntityDB db);

    void lockForWrite(DataEntityDB db);

    void unlockForRead(DataEntityDB db);

    void unlockForWrite(DataEntityDB db);

    void lockAll();

    void unlockAll();

    Map<DataEntityDB, List<DataEntityCollection>> listCollections();

    void clear();

    void clearDatabase(DataEntityDB db);

    void copyDatabase(DataEntityDB sourceDB, DataEntityDB destinationDB);
}
