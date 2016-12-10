package org.apache.hadoop.tools.posum.data.mock.data;


import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.tools.posum.common.records.call.DatabaseCall;
import org.apache.hadoop.tools.posum.common.records.call.query.DatabaseQuery;
import org.apache.hadoop.tools.posum.common.records.dataentity.*;
import org.apache.hadoop.tools.posum.common.records.payload.Payload;
import org.apache.hadoop.tools.posum.common.records.payload.SimplePropertyPayload;
import org.apache.hadoop.tools.posum.common.util.PosumException;
import org.apache.hadoop.tools.posum.common.util.Utils;
import org.apache.hadoop.tools.posum.data.mock.data.predicate.QueryPredicate;
import org.apache.hadoop.tools.posum.data.mock.data.predicate.QueryPredicateFactory;
import org.apache.hadoop.tools.posum.data.core.LockBasedDataStore;
import org.bson.types.ObjectId;

import java.beans.IntrospectionException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.LinkedHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MockDataStoreImpl implements LockBasedDataStore {

    private Map<DataEntityDB, DBAssets> dbRegistry = new ConcurrentHashMap<>();
    private ReentrantReadWriteLock masterLock = new ReentrantReadWriteLock();

    private static class DBAssets {
        ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        Map<DataEntityCollection, Map<String, ? extends GeneralDataEntity>> collections =
                new ConcurrentHashMap<>(DataEntityCollection.values().length);
    }

    private <T extends GeneralDataEntity> Map<String, T> getCollectionForRead(DataEntityDB db, DataEntityCollection collection) {
        DBAssets assets = getDatabaseAssets(db);
        if (assets.lock.getReadHoldCount() < 1 && !assets.lock.writeLock().isHeldByCurrentThread())
            throw new PosumException("No read session found for thread on " + db);
        return getCollection(assets, collection);
    }

    private <T extends GeneralDataEntity> Map<String, T> getCollectionForWrite(DataEntityDB db, DataEntityCollection collection) {
        DBAssets assets = getDatabaseAssets(db);
        if (!assets.lock.writeLock().isHeldByCurrentThread())
            throw new PosumException("No write session found for thread on " + db);
        return getCollection(assets, collection);
    }

    private DBAssets getDatabaseAssets(DataEntityDB db) {
        DBAssets assets = dbRegistry.get(db);
        synchronized (this) {
            if (assets == null) {
                assets = new DBAssets();
                dbRegistry.put(db, assets);
            }
        }
        return assets;
    }

    private <T extends GeneralDataEntity<T>> Map<String, T> getCollection(DBAssets assets, DataEntityCollection collection) {
        Map<String, ? extends GeneralDataEntity> dbCollection = assets.collections.get(collection);
        synchronized (this) {
            if (dbCollection == null) {
                dbCollection = new HashMap<>();
                assets.collections.put(collection, dbCollection);
            }
        }
        return (Map<String, T>) dbCollection;
    }


    @Override
    public <T extends GeneralDataEntity<T>> T findById(DataEntityDB db, DataEntityCollection collection, String id) {
        T ret = this.<T>getCollectionForRead(db, collection).get(id);
        if (ret == null)
            return null;
        return ret.copy();
    }

    @Override
    public <T extends GeneralDataEntity<T>> List<T> find(DataEntityDB db,
                                                         DataEntityCollection collection,
                                                         DatabaseQuery query,
                                                         String sortField,
                                                         boolean sortDescending,
                                                         int offsetOrZero,
                                                         int limitOrZero) {
        return new ArrayList<>(MockDataStoreImpl.findEntitiesByQuery(
                this.<T>getCollectionForRead(db, collection),
                query,
                sortField,
                sortDescending,
                offsetOrZero,
                limitOrZero
        ).values());
    }

    @Override
    public List<String> findIds(DataEntityDB db,
                                DataEntityCollection collection,
                                DatabaseQuery query,
                                String sortField,
                                boolean sortAsc,
                                int offsetOrZero,
                                int limitOrZero) {
        return new ArrayList<>(findEntitiesByQuery(
                this.getCollectionForRead(db, collection),
                query,
                sortField,
                sortAsc,
                offsetOrZero,
                limitOrZero
        ).keySet());
    }


    private static <T extends GeneralDataEntity<T>> Map<String, T> findEntitiesByQuery(Map<String, T> entities,
                                                                                       DatabaseQuery query,
                                                                                       String sortField,
                                                                                       boolean sortDescending,
                                                                                       int offsetOrZero,
                                                                                       int limitOrZero) {
        if (entities.size() < 1)
            return Collections.emptyMap();
        List<SimplePropertyPayload> relevant = new LinkedList<>();
        Map<String, T> results = new LinkedHashMap<>();
        Class entityClass = entities.values().iterator().next().getClass();
        QueryPredicate<? extends DatabaseQuery> predicate = QueryPredicateFactory.fromQuery(query);
        Set<String> relevantProperties = new HashSet<>(predicate.getCheckedProperties());
        if (sortField != null)
            relevantProperties.add(sortField);
        try {
            Map<String, Method> propertyReaders = Utils.getBeanPropertyReaders(entityClass, relevantProperties);
            for (Map.Entry<String, T> entry : entities.entrySet()) {
                if (predicate.check(entry.getValue(), propertyReaders)) {
                    Object sortablePropertyValue = null;
                    if (sortField != null) {
                        sortablePropertyValue = propertyReaders.get(sortField).invoke(entry.getValue());
                    }
                    relevant.add(SimplePropertyPayload.newInstance(entry.getKey(), sortablePropertyValue));
                }
            }
            if (sortField != null) {
                if (sortDescending)
                    Collections.sort(relevant, new Comparator<SimplePropertyPayload>() {
                        @Override
                        public int compare(SimplePropertyPayload o1, SimplePropertyPayload o2) {
                            return -o1.compareTo(o2);
                        }
                    });
                else
                    Collections.sort(relevant);
            }
            int skip = offsetOrZero >= 0 ? offsetOrZero : relevant.size() + offsetOrZero;
            int count = limitOrZero != 0 ? limitOrZero : relevant.size();
            for (SimplePropertyPayload next : relevant) {
                if (skip-- <= 0)
                    results.put(next.getName(), entities.get(next.getName()).copy());
                if (--count == 0)
                    break;
            }
        } catch (IntrospectionException | InvocationTargetException | IllegalAccessException e) {
            throw new PosumException("Reflection error while accessing entity properties", e);
        }
        return results;
    }

    @Override
    public <T extends GeneralDataEntity<T>> String store(DataEntityDB db, DataEntityCollection collection, T toStore) {
        if (toStore.getId() == null) {
            toStore.setId(ObjectId.get().toHexString());
        } else {
            if (findById(db, collection, toStore.getId()) != null) {
                throw new PosumException("Cannot insert duplicate key " + toStore.getId());
            }
        }
        getCollectionForWrite(db, collection).put(toStore.getId(), toStore);
        return toStore.getId();

    }

    @Override
    public <T extends GeneralDataEntity<T>> void storeAll(DataEntityDB db, DataEntityCollection collection, List<T> toStore) {
        for (T entity : toStore) {
            store(db, collection, entity);
        }
    }

    @Override
    public <T extends GeneralDataEntity<T>> String updateOrStore(DataEntityDB db, DataEntityCollection collection, T toUpdate) {
        boolean found = false;
        if (toUpdate.getId() != null)
            found = deleteReturnFound(db, collection, toUpdate.getId());
        String upsertedId = store(db, collection, toUpdate);
        // in order to keep the convention from mongodb (return the id only if it was inserted, not updated)
        return found ? null : upsertedId;
    }

    @Override
    public void delete(DataEntityDB db, DataEntityCollection collection, String id) {
        deleteReturnFound(db, collection, id);
    }

    private boolean deleteReturnFound(DataEntityDB db, DataEntityCollection collection, String id) {
        return getCollectionForWrite(db, collection).remove(id) != null;
    }

    @Override
    public void delete(DataEntityDB db, DataEntityCollection collection, DatabaseQuery query) {
        List<String> ids = findIds(db, collection, query, null, false, 0, 0);
        Map<String, ?> entities = getCollectionForWrite(db, collection);
        for (String id : ids) {
            entities.remove(id);
        }
    }

    @Override
    public String getRawDocuments(DataEntityDB db, DataEntityCollection collection, DatabaseQuery query) {
        //TODO
        throw new NotImplementedException();
    }

    @Override
    public <T extends Payload> T executeDatabaseCall(DatabaseCall<T> call, DataEntityDB db) {
        return call.executeCall(this, db);
    }

    @Override
    public Map<DataEntityDB, List<DataEntityCollection>> listCollections() {
        Map<DataEntityDB, List<DataEntityCollection>> ret = new HashMap<>(dbRegistry.size());
        for (Map.Entry<DataEntityDB, DBAssets> assetsEntry : dbRegistry.entrySet()) {
            List<DataEntityCollection> collections = new LinkedList<>();
            for (Map.Entry<DataEntityCollection, Map<String, ? extends GeneralDataEntity>> collectionEntry : assetsEntry.getValue().collections.entrySet()) {
                if (collectionEntry.getValue().size() > 0)
                    collections.add(collectionEntry.getKey());
                ret.put(assetsEntry.getKey(), collections);
            }
        }
        return ret;
    }

    @Override
    public void clear() {
        lockAll();
        dbRegistry.clear();
        unlockAll();
    }

    @Override
    public void clearDatabase(DataEntityDB db) {
        masterLock.readLock().lock();
        try {
            DBAssets dbAssets = getDatabaseAssets(db);
            if (dbAssets == null)
                return;
            dbAssets.lock.writeLock().lock();
            try {
                dbAssets.collections.clear();
            } finally {
                dbAssets.lock.writeLock().unlock();
            }
        } finally {
            masterLock.readLock().unlock();
        }
    }

    @Override
    public void copyDatabase(DataEntityDB sourceDB, DataEntityDB destinationDB) {
        masterLock.readLock().lock();
        try {
            DBAssets sourceAssets = getDatabaseAssets(sourceDB);
            clearDatabase(destinationDB);
            if (sourceAssets == null)
                return;
            sourceAssets.lock.readLock().lock();
            lockForWrite(destinationDB);
            try {

                for (Map.Entry<DataEntityCollection, Map<String, ? extends GeneralDataEntity>> collectionEntry :
                        sourceAssets.collections.entrySet()) {
                    storeAll(destinationDB, collectionEntry.getKey(), new ArrayList<>(collectionEntry.getValue().values()));
                }
            } finally {
                unlockForWrite(destinationDB);
                sourceAssets.lock.readLock().unlock();
            }
        } finally {
            masterLock.readLock().unlock();
        }
    }

    @Override
    public void lockForRead(DataEntityDB db) {
        masterLock.readLock().lock();
        getDatabaseAssets(db).lock.readLock().lock();
    }

    @Override
    public void lockForWrite(DataEntityDB db) {
        masterLock.readLock().lock();
        getDatabaseAssets(db).lock.writeLock().lock();
    }

    @Override
    public void unlockForRead(DataEntityDB db) {
        getDatabaseAssets(db).lock.readLock().unlock();
        masterLock.readLock().unlock();
    }

    @Override
    public void unlockForWrite(DataEntityDB db) {
        getDatabaseAssets(db).lock.writeLock().unlock();
        masterLock.readLock().unlock();
    }

    @Override
    public void lockAll() {
        masterLock.writeLock().lock();
    }

    @Override
    public void unlockAll() {
        masterLock.writeLock().unlock();
    }

}
