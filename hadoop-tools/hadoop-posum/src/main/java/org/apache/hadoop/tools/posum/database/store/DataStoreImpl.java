package org.apache.hadoop.tools.posum.database.store;

import com.mongodb.MongoClient;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.common.records.dataentity.*;
import org.apache.hadoop.tools.posum.common.util.PosumConfiguration;
import org.apache.hadoop.tools.posum.common.util.PosumException;
import org.apache.hadoop.tools.posum.common.util.Utils;
import org.apache.hadoop.tools.posum.database.client.Database;
import org.bson.Document;
import org.mongojack.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by ane on 2/9/16.
 */
public class DataStoreImpl implements LockBasedDataStore {

    private static Log logger = LogFactory.getLog(DataStoreImpl.class);

    private final Configuration conf;
    private MongoClient mongoClient;
    private DataEntityDB logDb = DataEntityDB.getLogs();
    private ReentrantReadWriteLock masterLock = new ReentrantReadWriteLock();
    private Map<DataEntityDB, DBAssets> dbRegistry = new ConcurrentHashMap<>(DataEntityDB.Type.values().length);

    private static class DBAssets {
        public ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        public Map<DataEntityCollection, JacksonDBCollection> collections = new ConcurrentHashMap<>(DataEntityCollection.values().length);
    }

    public DataStoreImpl(Configuration conf) {
        String url = conf.get(PosumConfiguration.DATABASE_URL, PosumConfiguration.DATABASE_URL_DEFAULT);
        if (url != null)
            mongoClient = new MongoClient(url);
        mongoClient = new MongoClient();
        this.conf = conf;
    }

    private <T extends GeneralDataEntity> JacksonDBCollection<T, String> getCollectionForRead(DataEntityDB db, DataEntityCollection collection) {
        DBAssets assets = getDatabaseAssets(db);
        if (assets.lock.getReadHoldCount() < 1 && !assets.lock.writeLock().isHeldByCurrentThread())
            throw new PosumException("No read session found for thread on " + db);
        return getCollection(db, assets, collection);
    }

    private <T extends GeneralDataEntity> JacksonDBCollection<T, String> getCollectionForWrite(DataEntityDB db, DataEntityCollection collection) {
        DBAssets assets = getDatabaseAssets(db);
        if (!assets.lock.writeLock().isHeldByCurrentThread())
            throw new PosumException("No write session found for thread on " + db);
        return getCollection(db, assets, collection);
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

    private <T extends GeneralDataEntity> JacksonDBCollection<T, String> getCollection(DataEntityDB db, DBAssets assets, DataEntityCollection collection) {
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
    public <T extends GeneralDataEntity> T findById(DataEntityDB db, DataEntityCollection collection, String id) {
        return this.<T>getCollectionForRead(db, collection).findOneById(id);
    }

    @Override
    public <T extends GeneralDataEntity> List<T> find(DataEntityDB db,
                                                      DataEntityCollection collection,
                                                      Map<String, Object> params,
                                                      String sortField,
                                                      boolean sortDescending,
                                                      int offsetOrZero,
                                                      int limitOrZero) {
        return find(db, collection, composeQuery(params), sortField, sortDescending, offsetOrZero, limitOrZero);
    }

    @Override
    public List<String> findIds(DataEntityDB db,
                                DataEntityCollection collection,
                                Map<String, Object> params,
                                String sortField,
                                boolean sortDescending,
                                int offsetOrZero,
                                int limitOrZero) {
        List rawList = find(db, collection, params, sortField, sortDescending, offsetOrZero, limitOrZero);
        List<String> ret = new ArrayList<>(rawList.size());
        for (Object rawObject : rawList) {
            ret.add(collection.getMappedClass().cast(rawObject).getId());
        }
        return ret;
    }

    private <T extends GeneralDataEntity> List<T> find(DataEntityDB db,
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

    private DBQuery.Query composeQuery(Map<String, Object> queryParams) {
        if (queryParams == null || queryParams.size() == 0)
            return DBQuery.empty();
        ArrayList<DBQuery.Query> paramList = new ArrayList<>(queryParams.size());
        for (Map.Entry<String, Object> param : queryParams.entrySet()) {
            paramList.add(DBQuery.is(param.getKey(), param.getValue()));
        }
        return DBQuery.and(paramList.toArray(new DBQuery.Query[queryParams.size()]));
    }

    @Override
    public <T extends GeneralDataEntity> String store(DataEntityDB db, DataEntityCollection collection, T toInsert) {
        WriteResult<T, String> result = this.<T>getCollectionForWrite(db, collection).insert(toInsert);
        return result.getSavedId();
    }

    @Override
    public <T extends GeneralDataEntity> String updateOrStore(DataEntityDB db,
                                                              DataEntityCollection collection,
                                                              T toUpdate) {
        WriteResult<T, String> result = this.<T>getCollectionForWrite(db, collection)
                .update(DBQuery.is("_id", toUpdate.getId()), toUpdate, true, false);
        return (String) result.getUpsertedId();
    }

    @Override
    public void delete(DataEntityDB db, DataEntityCollection collection, String id) {
        getCollectionForWrite(db, collection).removeById(id);
    }

    @Override
    public void delete(DataEntityDB db, DataEntityCollection collection, Map<String, Object> queryParams) {
        getCollectionForWrite(db, collection).remove(composeQuery(queryParams));
    }

    public String getRawDocumentList(String database, String collection, Map<String, Object> queryParams) {
        return mongoClient.getDatabase(database)
                .getCollection(collection)
                .find(new Document(queryParams)).toString();
    }

    public <T> void storeLogEntry(LogEntry<T> logEntry) {
        updateOrStore(logDb, logEntry.getType().getCollection(), logEntry);
    }

    public <T> List<LogEntry<T>> findLogs(LogEntry.Type type, long from, long to) {
        return find(logDb,
                type.getCollection(),
                DBQuery.and(DBQuery.greaterThan("timestamp", from),
                        DBQuery.lessThanEquals("timestamp", to),
                        DBQuery.is("type", type)),
                null,
                false,
                0,
                0
        );
    }

    public <T> List<LogEntry<T>> findLogs(LogEntry.Type type, long after) {
        return find(logDb,
                type.getCollection(),
                DBQuery.and(DBQuery.greaterThan("timestamp", after),
                        DBQuery.is("type", type)),
                null,
                false,
                0,
                0
        );
    }

    public <T> LogEntry<T> findReport(LogEntry.Type type) {
        return findById(logDb, type.getCollection(), type.name());
    }

    public <T> void storeLogReport(LogEntry<T> logReport) {
        logReport.setId(logReport.getType().name());
        storeLogEntry(logReport);
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

    @Override
    public Database bindTo(DataEntityDB db) {
        return Utils.exposeDataStoreAsBroker(this).bindTo(db);
    }

    @Override
    public Map<DataEntityDB, List<DataEntityCollection>> listExistingCollections() {
        throw new NotImplementedException();
    }

    @Override
    public void clear() {
        throw new NotImplementedException();
    }
}
