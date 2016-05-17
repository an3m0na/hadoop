package org.apache.hadoop.tools.posum.database.store;

import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityType;
import org.apache.hadoop.tools.posum.common.records.dataentity.GeneralDataEntity;
import org.bson.Document;
import org.mongojack.DBQuery;
import org.mongojack.JacksonDBCollection;
import org.mongojack.WriteResult;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Created by ane on 3/3/16.
 */
public class MongoJackConnector extends MongoConnector {

    private static final int MAX_DBS = 100;
    private static final int MAX_COLS = 100;

    private static Integer transientId = MAX_DBS;
    private HashMap<String, Integer> transientDbs = new HashMap<>();
    private ConcurrentSkipListMap<Integer, JacksonDBCollection> collections = new ConcurrentSkipListMap<>();

    public MongoJackConnector(String databaseUrl) {
        super(databaseUrl);
    }

    synchronized void addCollections(DataEntityDB db, DataEntityType... types) {
        Integer id = db.getId();
        if (db.isView()) {
            transientDbs.put(db.getName(), ++transientId);
            id = transientId;
        }
        for (DataEntityType type : types) {
            collections.put(id * MAX_COLS + type.getId(), JacksonDBCollection.wrap(
                    client.getDB(db.getName()).getCollection(type.getLabel()),
                    type.getMappedClass(),
                    String.class));
        }
    }

    synchronized void deleteCollections(DataEntityDB db) {
        Integer id = db.getId();
        if (db.isView()) {
            id = transientDbs.remove(db.getName());
        }
        Set<Integer> toDelete = collections.keySet().subSet(id * MAX_COLS, (id + 1) * MAX_COLS);
        for (Integer index : toDelete) {
            collections.remove(index);
        }
    }

    private <T> JacksonDBCollection<T, String> getCollection(DataEntityDB db, DataEntityType collection) {
        Integer id = (db.isView() ? transientDbs.get(db.getName()) : db.getId()) * MAX_COLS + collection.getId();
        return (JacksonDBCollection<T, String>) collections.get(id);
    }

    <T extends GeneralDataEntity> String insertObject(DataEntityDB db, DataEntityType collection, T object) {
        WriteResult<T, String> result = this.<T>getCollection(db, collection).insert(object);
        return result.getSavedId();
    }

    <T extends GeneralDataEntity> boolean updateObject(DataEntityDB db, DataEntityType collection, T object) {
        return this.<T>getCollection(db, collection).updateById(object.getId(), object).getN() == 1;
    }

    <T extends GeneralDataEntity> boolean upsertObject(DataEntityDB db, DataEntityType collection, T object) {
        Object upsertedId = this.<T>getCollection(db, collection)
                .update(DBQuery.is("_id", object.getId()), object, true, false).getUpsertedId();
        if (object.getId() != null)
            return object.getId().equals(upsertedId);
        else
            return upsertedId != null;
    }

    <T> void deleteObject(DataEntityDB db, DataEntityType collection, String id) {
        this.<T>getCollection(db, collection).removeById(id);
    }

    <T> void deleteObjects(DataEntityDB db, DataEntityType collection, String field, Object value) {
        this.<T>getCollection(db, collection).remove(DBQuery.is(field, value));
    }

    private DBQuery.Query composeQuery(Map<String, Object> queryParams) {
        ArrayList<DBQuery.Query> paramList = new ArrayList<>(queryParams.size());
        for (Map.Entry<String, Object> param : queryParams.entrySet()) {
            paramList.add(DBQuery.is(param.getKey(), param.getValue()));
        }
        return DBQuery.and(paramList.toArray(new DBQuery.Query[queryParams.size()]));
    }

    <T> void deleteObject(DataEntityDB db, DataEntityType collection, Map<String, Object> queryParams) {
        this.<T>getCollection(db, collection).remove(composeQuery(queryParams));
    }

    <T> T findObjectById(DataEntityDB db, DataEntityType collection, String id) {
        return this.<T>getCollection(db, collection).findOneById(id);
    }

    <T> List<T> findObjects(DataEntityDB db, DataEntityType collection, String field, Object value) {
        return this.<T>getCollection(db, collection).find(DBQuery.is(field, value)).toArray();
    }

    <T> List<T> findObjects(DataEntityDB db, DataEntityType collection, DBQuery.Query query) {
        if (query == null)
            return this.<T>getCollection(db, collection).find().toArray();
        return this.<T>getCollection(db, collection).find(query).toArray();
    }

    <T> List<T> findObjects(DataEntityDB db, DataEntityType collection, Map<String, Object> queryParams) {
        if (queryParams == null || queryParams.size() == 0)
            return this.<T>getCollection(db, collection).find().toArray();
        return findObjects(db, collection, composeQuery(queryParams));
    }

    String getRawDocumentList(String database, String collection, Map<String, Object> queryParams) {
        return client.getDatabase(database)
                .getCollection(collection)
                .find(new Document(queryParams)).toString();
    }
}
