package org.apache.hadoop.tools.posum.common.records.call;

import org.apache.hadoop.tools.posum.common.records.payload.Payload;
import org.apache.hadoop.tools.posum.database.client.ExtendedDataClientInterface;

/**
 * Created by ane on 7/29/16.
 */
public interface DatabaseCall<T extends Payload> {

    T executeCall(ExtendedDataClientInterface dataStore);
}
