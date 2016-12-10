package org.apache.hadoop.tools.posum.data.core;

import org.apache.hadoop.tools.posum.client.data.DataStore;
import org.apache.hadoop.tools.posum.common.records.call.query.DatabaseQuery;
import org.apache.hadoop.tools.posum.common.records.dataentity.*;

import java.util.List;
import java.util.Map;

public interface LockBasedDataStore extends DataStore{

    <T extends GeneralDataEntity<T>> T findById(DatabaseReference db, DataEntityCollection collection, String id);

    <T extends GeneralDataEntity<T>> List<T> find(DatabaseReference db,
                                               DataEntityCollection collection,
                                               DatabaseQuery query,
                                               String sortField,
                                               boolean sortDescending,
                                               int offsetOrZero,
                                               int limitOrZero);

    List<String> findIds(DatabaseReference db,
                         DataEntityCollection collection,
                         DatabaseQuery query,
                         String sortField,
                         boolean sortDescending,
                         int offsetOrZero,
                         int limitOrZero);

    <T extends GeneralDataEntity<T>> String store(DatabaseReference db, DataEntityCollection collection, T toStore);

    <T extends GeneralDataEntity<T>> void storeAll(DatabaseReference db, DataEntityCollection collection, List<T> toStore);

    <T extends GeneralDataEntity<T>> String updateOrStore(DatabaseReference db, DataEntityCollection apps, T toUpdate);

    void delete(DatabaseReference db, DataEntityCollection collection, String id);

    void delete(DatabaseReference db, DataEntityCollection collection, DatabaseQuery query);

    String getRawDocuments(DatabaseReference db, DataEntityCollection collection, DatabaseQuery query);

    void lockForRead(DatabaseReference db);

    void lockForWrite(DatabaseReference db);

    void unlockForRead(DatabaseReference db);

    void unlockForWrite(DatabaseReference db);

    void lockAll();

    void unlockAll();

    Map<DatabaseReference, List<DataEntityCollection>> listCollections();

    void clear();

    void clearDatabase(DatabaseReference db);

    void copyDatabase(DatabaseReference sourceDB, DatabaseReference destinationDB);
}
