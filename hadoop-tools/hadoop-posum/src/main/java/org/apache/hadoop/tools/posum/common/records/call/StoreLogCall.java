package org.apache.hadoop.tools.posum.common.records.call;

import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityFactory;
import org.apache.hadoop.tools.posum.common.records.dataentity.LogEntry;
import org.apache.hadoop.tools.posum.common.records.payload.Payload;
import org.apache.hadoop.tools.posum.common.records.payload.SimplePropertyPayload;
import org.apache.hadoop.tools.posum.database.store.LockBasedDataStore;
import org.apache.hadoop.yarn.util.Records;

/**
 * Created by ane on 7/29/16.
 */
public abstract class StoreLogCall extends LockBasedDatabaseCallImpl<SimplePropertyPayload> {
    public static StoreLogCall newInstance(LogEntry logEntry) {
        StoreLogCall call = Records.newRecord(StoreLogCall.class);
        call.setLogEntry(logEntry);
        return call;
    }

    public static StoreLogCall newInstance(String simpleString) {
        return newInstance(DataEntityFactory.getNewLogEntry(LogEntry.Type.GENERAL,
                SimplePropertyPayload.newInstance("", simpleString)));
    }

    public abstract <T extends Payload> LogEntry<T> getLogEntry();

    public abstract void setLogEntry(LogEntry logEntry);


    @Override
    public SimplePropertyPayload execute(LockBasedDataStore dataStore, DataEntityDB db) {
        if (db == null || !db.isOfType(DataEntityDB.Type.SIMULATION)) {
            // do not store unintended logs from simulations
            return SimplePropertyPayload.newInstance("logId",
                    dataStore.store(DataEntityDB.getLogs(), getLogEntry().getType().getCollection(), getLogEntry()));
        }
        return SimplePropertyPayload.newInstance("logId", (String) null);
    }

    @Override
    protected void lockDatabase(LockBasedDataStore dataStore, DataEntityDB db) {
        if (db == null || !db.isOfType(DataEntityDB.Type.SIMULATION))
            // only lock if it is not a simulation
            dataStore.lockForWrite(DataEntityDB.getLogs());
    }

    @Override
    protected void unlockDatabase(LockBasedDataStore dataStore, DataEntityDB db) {
        if (db == null || !db.isOfType(DataEntityDB.Type.SIMULATION))
            // only unlock if it is not a simulation
            dataStore.unlockForWrite(DataEntityDB.getLogs());
    }
}
