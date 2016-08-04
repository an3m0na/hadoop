package org.apache.hadoop.tools.posum.database.store;

import com.mongodb.MongoClient;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.GeneralDataEntity;
import org.bson.Document;
import org.mongojack.*;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Created by ane on 3/3/16.
 */
public class MongoDBConnector {

    private static Log logger = LogFactory.getLog(MongoDBConnector.class);

    private static final int MAX_DBS = 100;
    private static final int MAX_COLS = 100;

    private MongoClient client;

    private static Integer transientId = MAX_DBS;
    private HashMap<String, Integer> transientDbs = new HashMap<>();
    private ConcurrentSkipListMap<Integer, JacksonDBCollection> collections = new ConcurrentSkipListMap<>();

    public MongoDBConnector(String databaseUrl) {
        if (databaseUrl != null)
            client = new MongoClient(databaseUrl);
        client = new MongoClient();
    }

    synchronized void addDatabase(DataEntityDB db, DataEntityCollection... types) {
        Integer id = db.getId();
        if (db.isView()) {
            transientDbs.put(db.getName(), ++transientId);
            id = transientId;
        }
        for (DataEntityCollection type : types) {
            collections.put(id * MAX_COLS + type.getId(), JacksonDBCollection.wrap(
                    client.getDB(db.getName()).getCollection(type.getLabel()),
                    type.getMappedClass(),
                    String.class));
        }
        logger.trace("Collections are:" + collections);
    }

    synchronized void dropDatabase(DataEntityDB db) {
        Integer id = db.getId();
        if (db.isView()) {
            id = transientDbs.remove(db.getName());
        }
        Set<Integer> toDelete = collections.keySet().subSet(id * MAX_COLS, (id + 1) * MAX_COLS);
        for (Integer index : toDelete) {
            collections.remove(index);
        }
        client.getDB(db.getName()).dropDatabase();
    }

    private <T> JacksonDBCollection<T, String> getCollection(DataEntityDB db, DataEntityCollection collection) {
        Integer id = (db.isView() ? transientDbs.get(db.getName()) : db.getId()) * MAX_COLS + collection.getId();
        return (JacksonDBCollection<T, String>) collections.get(id);
    }

    <T extends GeneralDataEntity> String insertObject(DataEntityDB db, DataEntityCollection collection, T object) {
        WriteResult<T, String> result = this.<T>getCollection(db, collection).insert(object);
        return result.getSavedId();
    }

    <T extends GeneralDataEntity> boolean upsertObject(DataEntityDB db, DataEntityCollection collection, T object) {
        Object upsertedId = this.<T>getCollection(db, collection)
                .update(DBQuery.is("_id", object.getId()), object, true, false).getUpsertedId();
        if (object.getId() != null)
            return object.getId().equals(upsertedId);
        else
            return upsertedId != null;
    }

    <T> void deleteObject(DataEntityDB db, DataEntityCollection collection, String id) {
        this.<T>getCollection(db, collection).removeById(id);
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

    <T> void deleteObject(DataEntityDB db, DataEntityCollection collection, Map<String, Object> queryParams) {
        this.<T>getCollection(db, collection).remove(composeQuery(queryParams));
    }

    <T> T findObjectById(DataEntityDB db, DataEntityCollection collection, String id) {
        return this.<T>getCollection(db, collection).findOneById(id);
    }

    <T> List<T> findObjects(DataEntityDB db, DataEntityCollection collection, DBQuery.Query query, String... fieldProjections) {
        JacksonDBCollection<T, String> dbCollection = getCollection(db, collection);
        DBCursor<T> cursor = query == null ? dbCollection.find(DBProjection.include(fieldProjections)) :
                dbCollection.find(query, DBProjection.include(fieldProjections));
        return cursor.toArray();
    }

    <T> List<T> findObjects(DataEntityDB db, DataEntityCollection collection, Map<String, Object> queryParams, String... fieldProjections) {
        return findObjects(db, collection, composeQuery(queryParams), fieldProjections);
    }

    private <T> List<T> findObjects(DataEntityDB db, DataEntityCollection collection, DBQuery.Query query, int offsetOrZero, int limitOrZero) {
        JacksonDBCollection<T, String> dbCollection = getCollection(db, collection);
        DBCursor<T> cursor = query == null ? dbCollection.find() : dbCollection.find(query);
        if (offsetOrZero != 0) {
            int skipValue = offsetOrZero > 0 ? offsetOrZero : cursor.count() + offsetOrZero;
            if (skipValue > 0)
                cursor.skip(skipValue);
        }
        if (limitOrZero != 0)
            cursor.limit(limitOrZero);
        return cursor.toArray();
    }

    <T> List<T> findObjects(DataEntityDB db, DataEntityCollection collection, Map<String, Object> queryParams, int offsetOrZero, int limitOrZero) {
        return findObjects(db, collection, composeQuery(queryParams), offsetOrZero, limitOrZero);
    }

    String getRawDocumentList(String database, String collection, Map<String, Object> queryParams) {
        return client.getDatabase(database)
                .getCollection(collection)
                .find(new Document(queryParams)).toString();
    }
}
