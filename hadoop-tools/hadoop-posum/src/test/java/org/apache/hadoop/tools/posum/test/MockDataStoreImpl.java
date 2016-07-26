package org.apache.hadoop.tools.posum.test;


import org.apache.hadoop.tools.posum.common.records.dataentity.*;
import org.apache.hadoop.tools.posum.common.util.POSUMException;
import org.apache.hadoop.tools.posum.common.util.Utils;
import org.apache.hadoop.tools.posum.database.client.DBImpl;
import org.apache.hadoop.tools.posum.database.client.DBInterface;
import org.bson.types.ObjectId;

import java.lang.reflect.Method;
import java.util.*;

/**
 * Created by ane on 2/10/16.
 */
public class MockDataStoreImpl implements MockDataStore {

    private Map<DataEntityDB, Map<DataEntityType, List<? extends GeneralDataEntity>>> storedEntities = new HashMap<>();

    public MockDataStoreImpl() {
        for (DataEntityDB.Type type : DataEntityDB.Type.values()) {
            Map<DataEntityType, List<? extends GeneralDataEntity>> dbEntities = new HashMap<>();
            for (DataEntityType dataEntityType : DataEntityType.values()) {
                dbEntities.put(dataEntityType, new LinkedList<GeneralDataEntity>());
            }
            storedEntities.put(DataEntityDB.newInstance(type), dbEntities);
        }
    }

    private <T extends GeneralDataEntity> List<T> getTypedEntities(DataEntityDB db, DataEntityType collection) {
        return (List<T>) storedEntities.get(db).get(collection);
    }


    @Override
    public <T extends GeneralDataEntity> T findById(DataEntityDB db, DataEntityType collection, String id) {
        for (T entity : this.<T>getTypedEntities(db, collection)) {
            if (entity.getId().equals(id))
                return entity;
        }
        return null;
    }

    @Override
    public List<String> listIds(DataEntityDB db, DataEntityType collection, Map<String, Object> queryParams) {
        List<? extends GeneralDataEntity> entities = find(db, collection, queryParams, 0, 0);
        List<String> ret = new ArrayList<>(entities.size());
        for (GeneralDataEntity entity : entities) {
            ret.add(entity.getId());
        }
        return ret;
    }

    @Override
    public <T extends GeneralDataEntity> List<T> find(DataEntityDB db, DataEntityType collection, Map<String, Object> queryParams, int offset, int limit) {
        List<T> entities = findEntitiesByParams(this.<T>getTypedEntities(db, collection), queryParams);
        if (offset != 0) {
            int skipValue = offset > 0 ? offset : entities.size() + offset;
            if (skipValue > 0)
                for (int i = skipValue; i > 0; i--)
                    entities.remove(0);
        }
        if (limit != 0)
            for (int i = limit; i < entities.size(); i++)
                entities.remove(i);
        return entities;
    }

    private static <T> List<T> findEntitiesByParams(List<T> entities,
                                                    Map<String, Object> params) {
        if (entities.size() < 1)
            return Collections.emptyList();
        List<T> results = new LinkedList<>();
        try {
            Map<String, Method> propertyReaders =
                    Utils.getBeanPropertyReaders(entities.get(0).getClass(), params.keySet());
            for (T entity : entities)
                if(Utils.checkBeanPropertiesMatch(entity, propertyReaders, params))
                    results.add(entity);
        } catch (Exception e) {
            throw new POSUMException("Exception occured during findByParams " + e);
        }
        return results;
    }

    @Override
    public <T extends GeneralDataEntity> String store(DataEntityDB db, DataEntityType collection, T toInsert) {
        if (toInsert.getId() == null) {
            toInsert.setId(ObjectId.get().toHexString());
        } else {
            if (findById(db, collection, toInsert.getId()) != null) {
                throw new POSUMException("Duplicate id " + toInsert.getId());
            }
        }
        this.getTypedEntities(db, collection).add(toInsert);
        return toInsert.getId();
    }

    @Override
    public <T extends GeneralDataEntity> boolean updateOrStore(DataEntityDB db, DataEntityType collection, T toUpdate) {
        if (toUpdate.getId() == null) {
            store(db, collection, toUpdate);
            return true;
        }
        delete(db, collection, toUpdate.getId());
        store(db, collection, toUpdate);
        return true;
    }

    @Override
    public void delete(DataEntityDB db, DataEntityType collection, String id) {
        for (Iterator<GeneralDataEntity> iterator = getTypedEntities(db, collection).iterator(); iterator.hasNext(); ) {
            if (iterator.next().getId().equals(id))
                iterator.remove();
        }
    }

    @Override
    public void delete(DataEntityDB db, DataEntityType collection, Map<String, Object> queryParams) {
        List<String> ids = listIds(db, collection, queryParams);
        for (Iterator<GeneralDataEntity> iterator = getTypedEntities(db, collection).iterator(); iterator.hasNext(); ) {
            if (ids.contains(iterator.next().getId()))
                iterator.remove();
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
        DataEntityType type = forHistory ? DataEntityType.JOB_HISTORY : DataEntityType.JOB;
        JobProfile job = findById(db, type, jobId);
        if (job == null)
            throw new POSUMException("Could not find job to save flex-fields: " + jobId);

        job.getFlexFields().putAll(newFields);
        updateOrStore(db, type, job);
    }


}
