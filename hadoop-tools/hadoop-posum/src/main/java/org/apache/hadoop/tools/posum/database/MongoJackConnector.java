package org.apache.hadoop.tools.posum.database;

import com.mongodb.DB;
import org.mongojack.DBQuery;
import org.mongojack.JacksonDBCollection;
import org.mongojack.WriteResult;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by ane on 3/3/16.
 */
public class MongoJackConnector extends MongoConnector {

    Map<Integer, JacksonDBCollection> collections = new HashMap<>();
    DB deprecatedDb;

    public MongoJackConnector(String database) {
        super(database);
        deprecatedDb = client.getDB(db.getName());
    }

    public void addCollection(DataCollection collection) {
        collections.put(collection.getId(),
                JacksonDBCollection.wrap(deprecatedDb.getCollection(collection.getLabel()),
                        collection.getMappedClass(),
                        String.class));
    }

    public <T> String insertObject(T object) {
        DataCollection collection = DataCollection.getByClass(object.getClass());
        WriteResult<T, String> result = collections.get(collection).insert(object);
        return result.getSavedId();
    }

    public <T> T findObjectById(DataCollection collection, String id) {
        return (T) collections.get(collection).findOneById(id);
    }

    public <T> T findObjects(DataCollection collection, DBQuery.Query query) {
        return (T) collections.get(collection).find(query);
    }
}
