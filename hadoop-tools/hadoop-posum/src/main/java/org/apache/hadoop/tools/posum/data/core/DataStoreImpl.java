package org.apache.hadoop.tools.posum.data.core;

import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.common.records.call.DatabaseCall;
import org.apache.hadoop.tools.posum.common.records.call.query.CompositionQuery;
import org.apache.hadoop.tools.posum.common.records.call.query.DatabaseQuery;
import org.apache.hadoop.tools.posum.common.records.call.query.PropertyRangeQuery;
import org.apache.hadoop.tools.posum.common.records.call.query.PropertyValueQuery;
import org.apache.hadoop.tools.posum.common.records.dataentity.*;
import org.apache.hadoop.tools.posum.common.records.payload.Payload;
import org.apache.hadoop.tools.posum.common.records.payload.SimplePropertyPayload;
import org.apache.hadoop.tools.posum.common.util.PosumConfiguration;
import org.apache.hadoop.tools.posum.common.util.PosumException;
import org.bson.Document;
import org.mongojack.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.hadoop.tools.posum.common.util.Utils.ID_FIELD;

public class DataStoreImpl implements LockBasedDataStore {

    private static Log logger = LogFactory.getLog(DataStoreImpl.class);

    private MongoClient mongoClient;
    private ReentrantReadWriteLock masterLock = new ReentrantReadWriteLock();
    private Map<DatabaseReference, DBAssets> dbRegistry = new ConcurrentHashMap<>(DatabaseReference.Type.values().length);

    private static class DBAssets {
        public ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        public Map<DataEntityCollection, JacksonDBCollection> collections = new ConcurrentHashMap<>(DataEntityCollection.values().length);
    }

    public DataStoreImpl(Configuration conf) {
        String url = conf.get(PosumConfiguration.DATABASE_URL, PosumConfiguration.DATABASE_URL_DEFAULT);
        if (url != null)
            mongoClient = new MongoClient(url);
        mongoClient = new MongoClient();
    }

    private <T extends GeneralDataEntity> JacksonDBCollection<T, String> getCollectionForRead(DatabaseReference db, DataEntityCollection collection) {
        DBAssets assets = getDatabaseAssets(db);
        if (assets.lock.getReadHoldCount() < 1 && !assets.lock.writeLock().isHeldByCurrentThread())
            throw new PosumException("No read session found for thread on " + db);
        return getCollection(db, assets, collection);
    }

    private <T extends GeneralDataEntity> JacksonDBCollection<T, String> getCollectionForWrite(DatabaseReference db, DataEntityCollection collection) {
        DBAssets assets = getDatabaseAssets(db);
        if (!assets.lock.writeLock().isHeldByCurrentThread())
            throw new PosumException("No write session found for thread on " + db);
        return getCollection(db, assets, collection);
    }

    private DBAssets getDatabaseAssets(DatabaseReference db) {
        DBAssets assets = dbRegistry.get(db);
        synchronized (this) {
            if (assets == null) {
                assets = new DBAssets();
                dbRegistry.put(db, assets);
            }
        }
        return assets;
    }

    private <T extends GeneralDataEntity<T>> JacksonDBCollection<T, String> getCollection(DatabaseReference db, DBAssets assets, DataEntityCollection collection) {
        JacksonDBCollection dbCollection = assets.collections.get(collection);
        synchronized (this) {
            if (dbCollection == null) {
                dbCollection = JacksonDBCollection.wrap(
                        mongoClient.getDB(db.getName()).getCollection(collection.getLabel()),
                        collection.getMappedClass(),
                        String.class);
                assets.collections.put(collection, dbCollection);
            }
        }
        return (JacksonDBCollection<T, String>) dbCollection;
    }

    @Override
    public <T extends GeneralDataEntity<T>> T findById(DatabaseReference db, DataEntityCollection collection, String id) {
        return this.<T>getCollectionForRead(db, collection).findOneById(id);
    }

    @Override
    public <T extends GeneralDataEntity<T>> List<T> find(DatabaseReference db,
                                                         DataEntityCollection collection,
                                                         DatabaseQuery query,
                                                         String sortField,
                                                         boolean sortDescending,
                                                         int offsetOrZero,
                                                         int limitOrZero) {
        return find(db, collection, interpretQuery(query), sortField, sortDescending, offsetOrZero, limitOrZero);
    }

    @Override
    public List<String> findIds(DatabaseReference db,
                                DataEntityCollection collection,
                                DatabaseQuery query,
                                String sortField,
                                boolean sortDescending,
                                int offsetOrZero,
                                int limitOrZero) {
        List rawList = find(db, collection, query, sortField, sortDescending, offsetOrZero, limitOrZero);
        List<String> ret = new ArrayList<>(rawList.size());
        for (Object rawObject : rawList) {
            ret.add(collection.getMappedClass().cast(rawObject).getId());
        }
        return ret;
    }

    private <T extends GeneralDataEntity> List<T> find(DatabaseReference db,
                                                       DataEntityCollection collection,
                                                       DBQuery.Query query,
                                                       String sortField,
                                                       boolean sortDescending,
                                                       int offsetOrZero,
                                                       int limitOrZero) {
        JacksonDBCollection<T, String> dbCollection = getCollectionForRead(db, collection);
        DBCursor<T> cursor = getInitialCursor(dbCollection, query, null);
        cursor = applySorting(cursor, sortField, sortDescending);
        cursor = trim(cursor, offsetOrZero, limitOrZero);
        return cursor.toArray();
    }

    private <T extends GeneralDataEntity> DBCursor<T> getInitialCursor(JacksonDBCollection<T, String> dbCollection,
                                                                       DBQuery.Query query,
                                                                       String[] fieldsToInclude) {
        if (query == null)
            return dbCollection.find();
        if (fieldsToInclude == null || fieldsToInclude.length < 1)
            return dbCollection.find(query);
        else
            return dbCollection.find(query, DBProjection.include(fieldsToInclude));
    }

    private <T extends GeneralDataEntity> DBCursor<T> applySorting(DBCursor<T> cursor, String sortField, boolean desc) {
        if (sortField == null)
            return cursor;
        return cursor.sort(desc ? DBSort.desc(sortField) : DBSort.asc(sortField));
    }

    private <T extends GeneralDataEntity> DBCursor<T> trim(DBCursor<T> cursor, int offsetOrZero, int limitOrZero) {
        if (offsetOrZero != 0) {
            int skipValue = offsetOrZero > 0 ? offsetOrZero : cursor.count() + offsetOrZero;
            if (skipValue > 0)
                cursor.skip(skipValue);
        }
        if (limitOrZero != 0)
            cursor.limit(limitOrZero);
        return cursor;
    }

    private DBQuery.Query interpretQuery(PropertyValueQuery query) {
        SimplePropertyPayload property = query.getProperty();
        switch (query.getType()) {
            case IS:
                return DBQuery.is(property.getName(), property.getValue());
            case IS_NOT:
                return DBQuery.notEquals(property.getName(), property.getValue());
            case LESS:
                return DBQuery.lessThan(property.getName(), property.getValue());
            case LESS_OR_EQUAL:
                return DBQuery.lessThanEquals(property.getName(), property.getValue());
            case GREATER:
                return DBQuery.greaterThan(property.getName(), property.getValue());
            case GREATER_OR_EQUAL:
                return DBQuery.greaterThanEquals(property.getName(), property.getValue());
            default:
                throw new PosumException("PropertyValue query type not recognized: " + query.getType());
        }
    }

    private DBQuery.Query interpretQuery(CompositionQuery query) {
        DBQuery.Query[] innerQueries = new DBQuery.Query[query.getQueries().size()];
        int i = 0;
        for (DatabaseQuery innerQuery : query.getQueries()) {
            innerQueries[i++] = interpretQuery(innerQuery);
        }
        switch (query.getType()) {
            case AND:
                return DBQuery.and(innerQueries);
            case OR:
                return DBQuery.or(innerQueries);
            default:
                throw new PosumException("Composition query type not recognized: " + query.getType());
        }
    }

    private DBQuery.Query interpretQuery(PropertyRangeQuery query) {
        return DBQuery.in(query.getPropertyName(), query.getValues());
    }

    private DBQuery.Query interpretQuery(DatabaseQuery query) {
        if (query == null)
            return DBQuery.empty();
        if (query instanceof CompositionQuery)
            return interpretQuery((CompositionQuery) query);
        if (query instanceof PropertyValueQuery)
            return interpretQuery((PropertyValueQuery) query);
        if (query instanceof PropertyRangeQuery)
            return interpretQuery((PropertyRangeQuery) query);
        throw new PosumException("Query type not recognized: " + query.getClass());
    }

    @Override
    public <T extends GeneralDataEntity<T>> String store(DatabaseReference db, DataEntityCollection collection, T toStore) {
        WriteResult<T, String> result = this.<T>getCollectionForWrite(db, collection).insert(toStore);
        return result.getSavedId();
    }

    @Override
    public <T extends GeneralDataEntity<T>> void storeAll(DatabaseReference db, DataEntityCollection collection, List<T> toStore) {
        this.<T>getCollectionForWrite(db, collection).insert(toStore);
    }

    @Override
    public <T extends GeneralDataEntity<T>> String updateOrStore(DatabaseReference db,
                                                                 DataEntityCollection collection,
                                                                 T toUpdate) {
        WriteResult<T, String> result = this.<T>getCollectionForWrite(db, collection)
                .update(DBQuery.is(ID_FIELD, toUpdate.getId()), toUpdate, true, false);
        return (String) result.getUpsertedId();
    }

    @Override
    public void delete(DatabaseReference db, DataEntityCollection collection, String id) {
        getCollectionForWrite(db, collection).removeById(id);
    }

    @Override
    public void delete(DatabaseReference db, DataEntityCollection collection, DatabaseQuery query) {
        getCollectionForWrite(db, collection).remove(interpretQuery(query));
    }

    @Override
    public String getRawDocuments(DatabaseReference db, DataEntityCollection collection, DatabaseQuery query) {
        DBObject queryObject = getCollectionForRead(db, collection).serializeQuery(interpretQuery(query));
        return mongoClient.getDatabase(db.getName())
                .getCollection(collection.getLabel())
                .find(new Document(queryObject.toMap())).toString();
    }

    @Override
    public void lockForRead(DatabaseReference db) {
        masterLock.readLock().lock();
        getDatabaseAssets(db).lock.readLock().lock();
    }

    @Override
    public void lockForWrite(DatabaseReference db) {
        masterLock.readLock().lock();
        getDatabaseAssets(db).lock.writeLock().lock();
    }

    @Override
    public void unlockForRead(DatabaseReference db) {
        getDatabaseAssets(db).lock.readLock().unlock();
        masterLock.readLock().unlock();
    }

    @Override
    public void unlockForWrite(DatabaseReference db) {
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

    @Override
    public <T extends Payload> T executeDatabaseCall(DatabaseCall<T> call, DatabaseReference db) {
        return call.executeCall(this, db);
    }

    @Override
    public Map<DatabaseReference, List<DataEntityCollection>> listCollections() {
        Map<DatabaseReference, List<DataEntityCollection>> ret = new HashMap<>(dbRegistry.size());
        for (Map.Entry<DatabaseReference, DBAssets> assetsEntry : dbRegistry.entrySet()) {
            List<DataEntityCollection> collections = new ArrayList<>(DataEntityCollection.values().length / 2);
            for (Map.Entry<DataEntityCollection, JacksonDBCollection> collectionEntry :
                    assetsEntry.getValue().collections.entrySet()) {
                if (collectionEntry.getValue().count() > 0) {
                    collections.add(collectionEntry.getKey());
                }
            }
            if (collections.size() > 0)
                ret.put(assetsEntry.getKey(), collections);
        }
        return ret;
    }

    @Override
    public void clear() {
        lockAll();
        try {
            for (DatabaseReference db : dbRegistry.keySet())
                clearDatabase(db);
            dbRegistry.clear();
        } finally {
            unlockAll();
        }
    }

    @Override
    public void clearDatabase(DatabaseReference db) {
        masterLock.readLock().lock();
        try {
            DBAssets dbAssets = getDatabaseAssets(db);
            if (dbAssets == null)
                return;
            dbAssets.lock.writeLock().lock();
            try {
                mongoClient.getDatabase(db.getName()).drop();
                dbAssets.collections.clear();
            } finally {
                dbAssets.lock.writeLock().unlock();
            }
        } finally {
            masterLock.readLock().unlock();
        }
    }

    @Override
    public void copyDatabase(DatabaseReference sourceDB, DatabaseReference destinationDB) {
        masterLock.readLock().lock();
        try {
            DBAssets sourceAssets = getDatabaseAssets(sourceDB);
            clearDatabase(destinationDB);
            if (sourceAssets == null)
                return;
            sourceAssets.lock.readLock().lock();
            try {
                for (Map.Entry<DataEntityCollection, JacksonDBCollection> collectionEntry :
                        sourceAssets.collections.entrySet()) {
                    try {
                        lockForWrite(destinationDB);
                        List entities = collectionEntry.getValue().find().toArray();
                        if (entities.size() > 0)
                            storeAll(destinationDB, collectionEntry.getKey(), entities);
                    } finally {
                        unlockForWrite(destinationDB);
                    }
                }
            } finally {
                sourceAssets.lock.readLock().unlock();
            }
        } finally {
            masterLock.readLock().unlock();
        }
    }
}
