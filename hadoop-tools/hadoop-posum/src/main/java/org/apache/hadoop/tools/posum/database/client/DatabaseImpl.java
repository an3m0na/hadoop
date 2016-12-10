package org.apache.hadoop.tools.posum.database.client;

import org.apache.hadoop.tools.posum.common.records.call.DatabaseCall;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.tools.posum.common.records.payload.Payload;

public class DatabaseImpl implements Database {

    private final DataBroker dataBroker;
    private DataEntityDB selectedDatabase;

    public DatabaseImpl(DataBroker dataBroker, DataEntityDB selectedDatabase) {
        this.dataBroker = dataBroker;
        this.selectedDatabase = selectedDatabase;
    }

    @Override
    public <T extends Payload> T executeDatabaseCall(DatabaseCall<T> call) {
        return dataBroker.executeDatabaseCall(call, selectedDatabase);
    }

    @Override
    public void clear() {
        dataBroker.clearDatabase(selectedDatabase);
    }

    @Override
    public DataEntityDB getTarget() {
        return selectedDatabase;
    }
}
