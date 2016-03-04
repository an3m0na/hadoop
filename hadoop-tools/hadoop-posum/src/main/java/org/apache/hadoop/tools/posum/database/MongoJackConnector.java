package org.apache.hadoop.tools.posum.database;

import com.mongodb.DB;
import org.mongojack.DBQuery;
import org.mongojack.JacksonDBCollection;
import org.mongojack.WriteResult;

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

    public <T> T findObjectById(DataCollection collection, String id) {
        return this.<T>getCollection(collection).findOneById(id);
    }

    public <T> List<T> findObjects(DataCollection collection, DBQuery.Query query) {
        return this.<T>getCollection(collection).find(query).toArray();
    }
}
