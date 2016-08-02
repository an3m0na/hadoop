package org.apache.hadoop.tools.posum.common.records.call;

import org.apache.hadoop.tools.posum.common.records.payload.SimplePropertyPayload;

/**
 * Created by ane on 7/30/16.
 */
public abstract class UpdateOrStoreCall extends WriteCall{

    @Override
    public SimplePropertyPayload execute() {
        return SimplePropertyPayload.newInstance(
                "upsertedId",
                SimplePropertyPayload.PropertyType.BOOL,
                dataStore.updateOrStore(getEntityDB(), getEntityCollection(), getEntity())
        );
    }
}
