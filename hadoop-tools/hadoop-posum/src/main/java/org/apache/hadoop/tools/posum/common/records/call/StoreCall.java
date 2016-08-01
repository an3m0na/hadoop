package org.apache.hadoop.tools.posum.common.records.call;

import org.apache.hadoop.tools.posum.common.records.payload.SimplePropertyPayload;

/**
 * Created by ane on 7/29/16.
 */
public abstract class StoreCall extends WriteCall {

    @Override
    protected SimplePropertyPayload execute() {
        return SimplePropertyPayload.newInstance(
                "id",
                SimplePropertyPayload.PropertyType.STRING,
                dataStore.store(getEntityDB(), getEntityCollection(), getEntity())
        );
    }

}
