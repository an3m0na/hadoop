package org.apache.hadoop.tools.posum.database.store;

import com.mongodb.DB;
import org.apache.hadoop.tools.posum.common.records.profile.GeneralProfile;
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

    public MongoJackConnector(String databaseName, String databaseUrl) {
        super(databaseName, databaseUrl);
        deprecatedDb = client.getDB(db.getName());
    }

    public void addCollection(DataCollection collection) {
        collections.put(collection.getId(),
                JacksonDBCollection.wrap(deprecatedDb.getCollection(collection.getLabel()),
                        collection.getMappedClass(),
                        String.class));
    }


    <T> JacksonDBCollection<T, String> getCollection(DataCollection collection) {
        return (JacksonDBCollection<T, String>) collections.get(collection.getId());
    }

    public <T> String insertObject(DataCollection collection, T object) {
        WriteResult<T, String> result = this.<T>getCollection(collection).insert(object);
        return result.getSavedId();
    }

    public <T extends GeneralProfile> boolean updateObject(DataCollection collection, T object) {
        return this.<T>getCollection(collection).updateById(object.getId(), object).getN() == 1;
    }

    public <T extends GeneralProfile> boolean upsertObject(DataCollection collection, T object) {
        Object upsertedId = this.<T>getCollection(collection)
                .update(DBQuery.is("_id", object.getId()), object, true, false).getUpsertedId();
        if (object.getId() != null)
            return object.getId().equals(upsertedId);
        else
            return upsertedId != null;
    }

    public <T> void deleteObject(DataCollection collection, String id) {
        this.<T>getCollection(collection).removeById(id);
    }

    public <T> void deleteObjects(DataCollection collection, String field, Object value) {
        this.<T>getCollection(collection).remove(DBQuery.is(field, value));
    }

    private DBQuery.Query composeQuery(Map<String, Object> queryParams) {
        ArrayList<DBQuery.Query> paramList = new ArrayList<>(queryParams.size());
        for (Map.Entry<String, Object> param : queryParams.entrySet()) {
            paramList.add(DBQuery.is(param.getKey(), param.getValue()));
        }
        return DBQuery.and(paramList.toArray(new DBQuery.Query[queryParams.size()]));
    }

    public <T> void deleteObject(DataCollection collection, Map<String, Object> queryParams) {
        this.<T>getCollection(collection).remove(composeQuery(queryParams));
    }

    public <T> T findObjectById(DataCollection collection, String id) {
        return this.<T>getCollection(collection).findOneById(id);
    }

    public <T> List<T> findObjects(DataCollection collection, String field, Object value) {
        return this.<T>getCollection(collection).find(DBQuery.is(field, value)).toArray();
    }

    public <T> List<T> findObjects(DataCollection collection, DBQuery.Query query) {
        return this.<T>getCollection(collection).find(query).toArray();
    }

    public <T> List<T> findObjects(DataCollection collection, Map<String, Object> queryParams) {
        return findObjects(collection, composeQuery(queryParams));
    }
}
