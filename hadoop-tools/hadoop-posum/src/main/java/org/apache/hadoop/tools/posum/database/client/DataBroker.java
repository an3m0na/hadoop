package org.apache.hadoop.tools.posum.database.client;

import org.apache.hadoop.tools.posum.common.records.call.DatabaseCall;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.payload.Payload;

import java.util.List;
import java.util.Map;


/**
 * Created by ane on 7/28/16.
 */
public interface DataBroker {

    Database bindTo(DataEntityDB db);

    <T extends Payload> T executeDatabaseCall(DatabaseCall<T> call, DataEntityDB db);

    Map<DataEntityDB, List<DataEntityCollection>> listExistingCollections();

    void clear();
}