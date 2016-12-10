package org.apache.hadoop.tools.posum.common.records.payload;

import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.yarn.util.Records;

public abstract class DatabaseAlterationPayload implements Payload {

    public static DatabaseAlterationPayload newInstance(DataEntityDB db) {
        DatabaseAlterationPayload payload = Records.newRecord(DatabaseAlterationPayload.class);
        payload.setSourceDB(db);
        return payload;
    }

    public static DatabaseAlterationPayload newInstance(DataEntityDB sourceDB, DataEntityDB destinationDB) {
        DatabaseAlterationPayload payload = Records.newRecord(DatabaseAlterationPayload.class);
        payload.setSourceDB(sourceDB);
        payload.setDestinationDB(destinationDB);
        return payload;
    }

    public abstract DataEntityDB getSourceDB();

    public abstract void setSourceDB(DataEntityDB db);

    public abstract DataEntityDB getDestinationDB();

    public abstract void setDestinationDB(DataEntityDB db);

}
