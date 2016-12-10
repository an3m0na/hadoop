package org.apache.hadoop.tools.posum.client.data;

import org.apache.hadoop.tools.posum.common.records.call.DatabaseCall;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.payload.Payload;

import java.util.List;
import java.util.Map;


public interface DataStore {

    <T extends Payload> T executeDatabaseCall(DatabaseCall<T> call, DataEntityDB db);

    Map<DataEntityDB, List<DataEntityCollection>> listCollections();

    void clear();

    void clearDatabase(DataEntityDB db);

    void copyDatabase(DataEntityDB sourceDB, DataEntityDB destinationDB);

}
