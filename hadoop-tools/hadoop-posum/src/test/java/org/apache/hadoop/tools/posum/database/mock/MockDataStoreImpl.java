package org.apache.hadoop.tools.posum.database.mock;


import org.apache.hadoop.tools.posum.common.records.call.DatabaseCall;
import org.apache.hadoop.tools.posum.common.records.dataentity.*;
import org.apache.hadoop.tools.posum.common.records.payload.Payload;
import org.apache.hadoop.tools.posum.common.util.PosumException;
import org.apache.hadoop.tools.posum.common.util.Utils;
import org.apache.hadoop.tools.posum.database.client.DataBroker;
import org.apache.hadoop.tools.posum.database.client.Database;
import org.apache.hadoop.tools.posum.database.client.DatabaseImpl;
import org.apache.hadoop.tools.posum.database.store.DataStore;
import org.bson.types.ObjectId;

import java.beans.IntrospectionException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.LinkedHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by ane on 2/10/16.
 */
public class MockDataStoreImpl implements DataStore {

    private Map<DataEntityDB, Map<DataEntityCollection, Map<String, ? extends GeneralDataEntity>>> storedEntities =
            new ConcurrentHashMap<>();
    private ConcurrentHashMap<Integer, ReentrantReadWriteLock> locks = new ConcurrentHashMap<>();

    public MockDataStoreImpl() {
        for (DataEntityDB db : DataEntityDB.listByType()) {
            Map<DataEntityCollection, Map<String, ? extends GeneralDataEntity>> dbEntities = new HashMap<>();
            for (DataEntityCollection dataEntityCollection : DataEntityCollection.values()) {
                dbEntities.put(dataEntityCollection, new LinkedHashMap<String, GeneralDataEntity>());

            }
            storedEntities.put(db, dbEntities);
            locks.put(db.getId(), new ReentrantReadWriteLock());
        }
        locks.put(Integer.MAX_VALUE, new ReentrantReadWriteLock());
    }

    private <T extends GeneralDataEntity> Map<String, T> getTypedEntities(DataEntityDB db, DataEntityCollection collection) {
        return (Map<String, T>) storedEntities.get(db).get(collection);
    }


    @Override
    public <T extends GeneralDataEntity> T findById(DataEntityDB db, DataEntityCollection collection, String id) {
        locks.get(db.getId()).readLock().lock();
        try {
            return this.<T>getTypedEntities(db, collection).get(id);
        } finally {
            locks.get(db.getId()).readLock().unlock();
        }
    }

    @Override
    public List<String> listIds(DataEntityDB db, DataEntityCollection collection, Map<String, Object> queryParams) {
        locks.get(db.getId()).readLock().lock();
        try {
            return new ArrayList<>(findEntitiesByParams(this.getTypedEntities(db, collection), queryParams, 0, 0).keySet());
        } finally {
            locks.get(db.getId()).readLock().unlock();
        }
    }

    @Override
    public <T extends GeneralDataEntity> List<T> find(DataEntityDB db, DataEntityCollection collection, Map<String, Object> queryParams, int offsetOrZero, int limitOrZero) {
        locks.get(db.getId()).readLock().lock();
        try {
            return new ArrayList<>(findEntitiesByParams(this.<T>getTypedEntities(db, collection), queryParams, offsetOrZero, limitOrZero).values());
        } finally {
            locks.get(db.getId()).readLock().unlock();
        }
    }


    private static <T> Map<String, T> findEntitiesByParams(Map<String, T> entities,
                                                           Map<String, Object> params, int offsetOrZero, int limitOrZero) {
        if (entities.size() < 1)
            return Collections.emptyMap();
        Map<String, T> tmpResults = new LinkedHashMap<>();
        Map<String, T> results = new LinkedHashMap<>();
        Class entityClass = entities.values().iterator().next().getClass();
        try {
            Map<String, Method> propertyReaders =
                    Utils.getBeanPropertyReaders(entityClass, params.keySet());
            for (Map.Entry<String, T> entry : entities.entrySet()) {
                if (Utils.checkBeanPropertiesMatch(entry.getValue(), propertyReaders, params))
                    tmpResults.put(entry.getKey(), entry.getValue());
            }
            int skip = offsetOrZero >= 0 ? offsetOrZero : tmpResults.size() + offsetOrZero;
            int count = limitOrZero != 0 ? limitOrZero : tmpResults.size();
            for (Iterator<Map.Entry<String, T>> iterator = tmpResults.entrySet().iterator(); iterator.hasNext(); ) {
                Map.Entry<String, T> next = iterator.next();
                if (skip-- <= 0)
                    results.put(next.getKey(), next.getValue());
                if (--count == 0)
                    break;
            }

        } catch (IntrospectionException | InvocationTargetException | IllegalAccessException e) {
            throw new PosumException("Reflection error while accessing entity properties", e);
        }
        return results;
    }

    @Override
    public <T extends GeneralDataEntity> String store(DataEntityDB db, DataEntityCollection collection, T toInsert) {
        locks.get(db.getId()).writeLock().lock();
        try {
            if (toInsert.getId() == null) {
                toInsert.setId(ObjectId.get().toHexString());
            } else {
                if (findById(db, collection, toInsert.getId()) != null) {
                    throw new PosumException("Cannot insert duplicate key " + toInsert.getId());
                }
            }
            this.getTypedEntities(db, collection).put(toInsert.getId(), toInsert);
            return toInsert.getId();
        } finally {
            locks.get(db.getId()).writeLock().unlock();
        }
    }

    @Override
    public <T extends GeneralDataEntity> boolean updateOrStore(DataEntityDB db, DataEntityCollection collection, T toUpdate) {
        locks.get(db.getId()).writeLock().lock();
        try {
            if (toUpdate.getId() == null) {
                store(db, collection, toUpdate);
                return true;
            }
            delete(db, collection, toUpdate.getId());
            store(db, collection, toUpdate);

            return true;
        } finally {
            locks.get(db.getId()).writeLock().unlock();
        }
    }

    @Override
    public void delete(DataEntityDB db, DataEntityCollection collection, String id) {
        locks.get(db.getId()).writeLock().lock();
        try {

            getTypedEntities(db, collection).remove(id);
        } finally {
            locks.get(db.getId()).writeLock().unlock();
        }
    }

    @Override
    public void delete(DataEntityDB db, DataEntityCollection collection, Map<String, Object> queryParams) {
        locks.get(db.getId()).writeLock().lock();
        try {

            List<String> ids = listIds(db, collection, queryParams);
            Map<String, ?> entities = storedEntities.get(db).get(collection);
            for (String id : ids) {
                entities.remove(id);
            }
        } finally {
            locks.get(db.getId()).writeLock().unlock();
        }
    }

    @Override
    public Database bindTo(DataEntityDB db) {
        final DataStore thisStore = this;
        return new DatabaseImpl(new DataBroker() {
            @Override
            public Database bindTo(DataEntityDB db) {
                return new DatabaseImpl(this, db);
            }

            @Override
            public <T extends Payload> T executeDatabaseCall(DatabaseCall<T> call) {
                return call.executeCall(thisStore);
            }

            @Override
            public Map<DataEntityDB, List<DataEntityCollection>> listExistingCollections() {
                return thisStore.listExistingCollections();
            }

            @Override
            public void clear() {
                thisStore.clear();
            }
        }, db);
    }

    @Override
    public JobProfile getJobProfileForApp(DataEntityDB db, String appId, String user) {
        List<JobProfile> profiles;
        profiles = find(db, DataEntityCollection.JOB, Collections.singletonMap("appId", (Object) appId), 0, 0);
        if (profiles.size() == 1)
            return profiles.get(0);
        if (profiles.size() > 1)
            throw new PosumException("Found too many profiles in database for app " + appId);
        return null;
    }

    @Override
    public void saveFlexFields(DataEntityDB db, String jobId, Map<String, String> newFields, boolean forHistory) {
        locks.get(db.getId()).writeLock().lock();
        try {
            DataEntityCollection type = forHistory ? DataEntityCollection.JOB_HISTORY : DataEntityCollection.JOB;
            JobProfile job = findById(db, type, jobId);
            if (job == null)
                throw new PosumException("Could not find job to save flex-fields: " + jobId);

            job.getFlexFields().putAll(newFields);
            updateOrStore(db, type, job);
        } finally {
            locks.get(db.getId()).writeLock().unlock();
        }
    }

    @Override
    public Map<DataEntityDB, List<DataEntityCollection>> listExistingCollections() {
        Map<DataEntityDB, List<DataEntityCollection>> ret = new HashMap<>(storedEntities.size());
        for (Map.Entry<DataEntityDB, Map<DataEntityCollection, Map<String, ? extends GeneralDataEntity>>> dbMapEntry :
                storedEntities.entrySet()) {
            List<DataEntityCollection> collections = new LinkedList<>();
            for (Map.Entry<DataEntityCollection, Map<String, ? extends GeneralDataEntity>> collectionEntry :
                    dbMapEntry.getValue().entrySet()) {
                if (collectionEntry.getValue().size() > 0)
                    collections.add(collectionEntry.getKey());
            }
            ret.put(dbMapEntry.getKey(), collections);
        }
        return ret;
    }

    @Override
    public void clear() {
        for (Map.Entry<DataEntityDB, Map<DataEntityCollection, Map<String, ? extends GeneralDataEntity>>> dbMapEntry :
                storedEntities.entrySet()) {
            for (Map.Entry<DataEntityCollection, Map<String, ? extends GeneralDataEntity>> collectionEntry :
                    dbMapEntry.getValue().entrySet()) {
                collectionEntry.getValue().clear();
            }
        }
    }

    @Override
    public void lockForRead(DataEntityDB db) {
        locks.get(db.getId()).readLock().lock();
    }

    @Override
    public void lockForWrite(DataEntityDB db) {
        locks.get(db.getId()).writeLock().lock();
    }

    @Override
    public void unlockForRead(DataEntityDB db) {
        locks.get(db.getId()).readLock().unlock();
    }

    @Override
    public void unlockForWrite(DataEntityDB db) {
        locks.get(db.getId()).writeLock().unlock();
    }
}
