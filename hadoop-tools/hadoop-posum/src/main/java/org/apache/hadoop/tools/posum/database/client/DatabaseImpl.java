package org.apache.hadoop.tools.posum.database.client;

import org.apache.hadoop.tools.posum.common.records.call.DatabaseCall;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.tools.posum.common.records.payload.Payload;

/**
 * Created by ane on 8/3/16.
 */
public class DatabaseImpl implements Database {

    private final DataBroker dataBroker;
    private DataEntityDB selectedDatabase;

    public DatabaseImpl(DataBroker dataBroker, DataEntityDB selectedDatabase) {
        this.dataBroker = dataBroker;
        this.selectedDatabase = selectedDatabase;
    }

    @Override
    public <T extends Payload> T executeDatabaseCall(DatabaseCall<T> call) {
        //TODO move this to the execution call
        call.setDatabase(selectedDatabase);
        return dataBroker.executeDatabaseCall(call);
    }
}
