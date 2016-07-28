package org.apache.hadoop.tools.posum.test;


import org.apache.hadoop.tools.posum.common.records.dataentity.*;
import org.apache.hadoop.tools.posum.common.util.POSUMException;
import org.apache.hadoop.tools.posum.common.util.Utils;
import org.apache.hadoop.tools.posum.database.client.DBImpl;
import org.apache.hadoop.tools.posum.database.client.DBInterface;
import org.apache.hadoop.tools.posum.database.store.DataStoreExporter;
import org.apache.hadoop.tools.posum.database.store.DataStoreImporter;
import org.bson.types.ObjectId;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.LinkedHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by ane on 2/10/16.
 */
public class MockDataStoreImpl implements MockDataStore {

    private Map<DataEntityDB, Map<DataEntityType, Map<String, ? extends GeneralDataEntity>>> storedEntities =
            new ConcurrentHashMap<>();
    private ConcurrentHashMap<Integer, ReentrantReadWriteLock> locks = new ConcurrentHashMap<>();

    public MockDataStoreImpl() {
        for (DataEntityDB db : DataEntityDB.listByType()) {
            Map<DataEntityType, Map<String, ? extends GeneralDataEntity>> dbEntities = new HashMap<>();
            for (DataEntityType dataEntityType : DataEntityType.values()) {
                dbEntities.put(dataEntityType, new LinkedHashMap<String, GeneralDataEntity>());

            }
            storedEntities.put(db, dbEntities);
            locks.put(db.getId(), new ReentrantReadWriteLock());
        }
    }

    private <T extends GeneralDataEntity> Map<String, T> getTypedEntities(DataEntityDB db, DataEntityType collection) {
        return (Map<String, T>) storedEntities.get(db).get(collection);
    }


    @Override
    public <T extends GeneralDataEntity> T findById(DataEntityDB db, DataEntityType collection, String id) {
        locks.get(db.getId()).readLock().lock();
        try {
            return this.<T>getTypedEntities(db, collection).get(id);
        } finally {
            locks.get(db.getId()).readLock().unlock();
        }
    }

    @Override
    public List<String> listIds(DataEntityDB db, DataEntityType collection, Map<String, Object> queryParams) {
        locks.get(db.getId()).readLock().lock();
        try {
            return new ArrayList<>(findEntitiesByParams(this.getTypedEntities(db, collection), queryParams, 0, 0).keySet());
        } finally {
            locks.get(db.getId()).readLock().unlock();
        }
    }

    @Override
    public <T extends GeneralDataEntity> List<T> find(DataEntityDB db, DataEntityType collection, Map<String, Object> queryParams, int offset, int limit) {
        locks.get(db.getId()).readLock().lock();
        try {
            return new ArrayList<>(findEntitiesByParams(this.<T>getTypedEntities(db, collection), queryParams, offset, limit).values());
        } finally {
            locks.get(db.getId()).readLock().unlock();
        }
    }


    private static <T> Map<String, T> findEntitiesByParams(Map<String, T> entities,
                                                           Map<String, Object> params, int offset, int limit) {
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
            int skip = offset >= 0 ? offset : tmpResults.size() + offset;
            int count = limit != 0 ? limit : tmpResults.size();
            for (Iterator<Map.Entry<String, T>> iterator = tmpResults.entrySet().iterator(); iterator.hasNext(); ) {
                Map.Entry<String, T> next = iterator.next();
                if (skip-- <= 0)
                    results.put(next.getKey(), next.getValue());
                if (--count == 0)
                    break;
            }

        } catch (IntrospectionException | InvocationTargetException | IllegalAccessException e) {
            throw new POSUMException("Reflection error while accessing entity properties", e);
        }
        return results;
    }

    @Override
    public <T extends GeneralDataEntity> String store(DataEntityDB db, DataEntityType collection, T toInsert) {
        locks.get(db.getId()).writeLock().lock();
        try {
            if (toInsert.getId() == null) {
                toInsert.setId(ObjectId.get().toHexString());
            } else {
                if (findById(db, collection, toInsert.getId()) != null) {
                    throw new POSUMException("Duplicate id " + toInsert.getId());
                }
            }
            this.getTypedEntities(db, collection).put(toInsert.getId(), toInsert);
            return toInsert.getId();
        } finally {
            locks.get(db.getId()).writeLock().unlock();
        }
    }

    @Override
    public <T extends GeneralDataEntity> boolean updateOrStore(DataEntityDB db, DataEntityType collection, T toUpdate) {
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
    public void delete(DataEntityDB db, DataEntityType collection, String id) {
        locks.get(db.getId()).writeLock().lock();
        try {

            getTypedEntities(db, collection).remove(id);
        } finally {
            locks.get(db.getId()).writeLock().unlock();
        }
    }

    @Override
    public void delete(DataEntityDB db, DataEntityType collection, Map<String, Object> queryParams) {
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
    public DBInterface bindTo(DataEntityDB db) {
        return new DBImpl(db, this);
    }

    @Override
    public JobProfile getJobProfileForApp(DataEntityDB db, String appId, String user) {
        List<JobProfile> profiles;
        profiles = find(db, DataEntityType.JOB, Collections.singletonMap("appId", (Object) appId), 0, 0);
        if (profiles.size() == 1)
            return profiles.get(0);
        if (profiles.size() > 1)
            throw new POSUMException("Found too many profiles in database for app " + appId);
        return null;
    }

    @Override
    public void saveFlexFields(DataEntityDB db, String jobId, Map<String, String> newFields, boolean forHistory) {
        locks.get(db.getId()).writeLock().lock();
        try {
            DataEntityType type = forHistory ? DataEntityType.JOB_HISTORY : DataEntityType.JOB;
            JobProfile job = findById(db, type, jobId);
            if (job == null)
                throw new POSUMException("Could not find job to save flex-fields: " + jobId);

            job.getFlexFields().putAll(newFields);
            updateOrStore(db, type, job);
        } finally {
            locks.get(db.getId()).writeLock().unlock();
        }
    }


    @Override
    public void importData(String dataDumpPath) throws IOException {
        new DataStoreImporter(dataDumpPath).importTo(this);
    }

    @Override
    public void exportData(String dataDumpPath) throws IOException {
        new DataStoreExporter(this).exportTo(dataDumpPath);
    }

    @Override
    public Map<DataEntityDB, List<DataEntityType>> listExistingCollections() {
        Map<DataEntityDB, List<DataEntityType>> ret = new HashMap<>(storedEntities.size());
        for (Map.Entry<DataEntityDB, Map<DataEntityType, Map<String, ? extends GeneralDataEntity>>> dbMapEntry :
                storedEntities.entrySet()) {
            List<DataEntityType> collections = new LinkedList<>();
            for (Map.Entry<DataEntityType, Map<String, ? extends GeneralDataEntity>> collectionEntry :
                    dbMapEntry.getValue().entrySet()) {
                if (collectionEntry.getValue().size() > 0)
                    collections.add(collectionEntry.getKey());
            }
            ret.put(dbMapEntry.getKey(), collections);
        }
        return ret;
    }
}
