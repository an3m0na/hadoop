package org.apache.hadoop.tools.posum.database.client;

import org.apache.hadoop.tools.posum.common.records.call.DatabaseCall;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.tools.posum.common.records.payload.Payload;
import org.apache.hadoop.tools.posum.database.store.DataStore;

import java.util.List;
import java.util.Map;

/**
 * Created by ane on 8/3/16.
 */
public class DataStoreClient implements DataBroker {

    private DataStore dataStore;
    private DataEntityDB defaultDatabase;

    public DataStoreClient(DataStore dataStore) {
        this.dataStore = dataStore;
    }

    @Override
    public void bindTo(DataEntityDB db) {
        this.defaultDatabase = db;
    }

    @Override
    public <T extends Payload> T executeDatabaseCall(DatabaseCall<T> call) {
        if(call.getDatabase() == null)
            call.setDatabase(defaultDatabase);
        return call.executeCall(dataStore);
    }

    @Override
    public Map<DataEntityDB, List<DataEntityCollection>> listExistingCollections() {
        return dataStore.listExistingCollections();
    }

    @Override
    public void clear() {
        dataStore.clear();
    }
}
