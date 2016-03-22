package org.apache.hadoop.tools.posum.database.store;

import com.mongodb.DB;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityType;
import org.apache.hadoop.tools.posum.common.records.dataentity.GeneralDataEntity;
import org.mongojack.DBQuery;
import org.mongojack.JacksonDBCollection;
import org.mongojack.WriteResult;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by ane on 3/3/16.
 */
public class MongoJackConnector extends MongoConnector {

    Map<Integer, JacksonDBCollection> collections = new HashMap<>();
    DB deprecatedDb;

    public MongoJackConnector(String databaseName) {
        this(databaseName, null);
    }

    MongoJackConnector(String databaseName, String databaseUrl) {
        super(databaseName, databaseUrl);
        deprecatedDb = client.getDB(db.getName());
    }

    void addCollection(DataEntityType collection) {
        collections.put(collection.getId(),
                JacksonDBCollection.wrap(deprecatedDb.getCollection(collection.getLabel()),
                        collection.getMappedClass(),
                        String.class));
    }


    private <T> JacksonDBCollection<T, String> getCollection(DataEntityType collection) {
        return (JacksonDBCollection<T, String>) collections.get(collection.getId());
    }

    <T extends GeneralDataEntity> String insertObject(DataEntityType collection, T object) {
        WriteResult<T, String> result = this.<T>getCollection(collection).insert(object);
        return result.getSavedId();
    }

    <T extends GeneralDataEntity> boolean updateObject(DataEntityType collection, T object) {
        return this.<T>getCollection(collection).updateById(object.getId(), object).getN() == 1;
    }

    <T extends GeneralDataEntity> boolean upsertObject(DataEntityType collection, T object) {
        Object upsertedId = this.<T>getCollection(collection)
                .update(DBQuery.is("_id", object.getId()), object, true, false).getUpsertedId();
        if (object.getId() != null)
            return object.getId().equals(upsertedId);
        else
            return upsertedId != null;
    }

    <T> void deleteObject(DataEntityType collection, String id) {
        this.<T>getCollection(collection).removeById(id);
    }

    <T> void deleteObjects(DataEntityType collection, String field, Object value) {
        this.<T>getCollection(collection).remove(DBQuery.is(field, value));
    }

    private DBQuery.Query composeQuery(Map<String, Object> queryParams) {
        ArrayList<DBQuery.Query> paramList = new ArrayList<>(queryParams.size());
        for (Map.Entry<String, Object> param : queryParams.entrySet()) {
            paramList.add(DBQuery.is(param.getKey(), param.getValue()));
        }
        return DBQuery.and(paramList.toArray(new DBQuery.Query[queryParams.size()]));
    }

    <T> void deleteObject(DataEntityType collection, Map<String, Object> queryParams) {
        this.<T>getCollection(collection).remove(composeQuery(queryParams));
    }

    <T> T findObjectById(DataEntityType collection, String id) {
        return this.<T>getCollection(collection).findOneById(id);
    }

    <T> List<T> findObjects(DataEntityType collection, String field, Object value) {
        return this.<T>getCollection(collection).find(DBQuery.is(field, value)).toArray();
    }

    <T> List<T> findObjects(DataEntityType collection, DBQuery.Query query) {
        if (query == null)
            return this.<T>getCollection(collection).find().toArray();
        return this.<T>getCollection(collection).find(query).toArray();
    }

    <T> List<T> findObjects(DataEntityType collection, Map<String, Object> queryParams) {
        if (queryParams == null || queryParams.size() == 0)
            return this.<T>getCollection(collection).find().toArray();
        return findObjects(collection, composeQuery(queryParams));
    }
}
