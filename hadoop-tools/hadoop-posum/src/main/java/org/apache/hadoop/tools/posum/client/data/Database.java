package org.apache.hadoop.tools.posum.client.data;

import org.apache.hadoop.tools.posum.common.records.call.DatabaseCall;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.tools.posum.common.records.payload.Payload;

public class Database {

    private final DataStore dataStore;
    private DataEntityDB selectedDatabase;

    public Database(DataStore dataStore, DataEntityDB selectedDatabase) {
        this.dataStore = dataStore;
        this.selectedDatabase = selectedDatabase;
    }

    public <T extends Payload> T executeDatabaseCall(DatabaseCall<T> call) {
        return dataStore.executeDatabaseCall(call, selectedDatabase);
    }

    public void clear() {
        dataStore.clearDatabase(selectedDatabase);
    }

    public DataEntityDB getTarget() {
        return selectedDatabase;
    }

    public static Database extractFrom(DataStore dataStore, DataEntityDB db){
        return new Database(dataStore, db);
    }
}
