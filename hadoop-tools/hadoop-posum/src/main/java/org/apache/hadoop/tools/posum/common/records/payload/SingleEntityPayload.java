package org.apache.hadoop.tools.posum.common.records.payload;

import org.apache.hadoop.tools.posum.common.records.dataentity.GeneralDataEntity;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.yarn.util.Records;

/**
 * Created by ane on 3/20/16.
 */
public abstract class SingleEntityPayload implements Payload{

    public static SingleEntityPayload newInstance(DataEntityCollection collection, GeneralDataEntity object) {
        SingleEntityPayload payload = Records.newRecord(SingleEntityPayload.class);
        payload.setEntityCollection(collection);
        payload.setEntity(object);
        return payload;
    }

    public abstract DataEntityCollection getEntityCollection();

    public abstract void setEntityCollection(DataEntityCollection type);

    public abstract GeneralDataEntity getEntity();

    public abstract void setEntity(GeneralDataEntity entity);

}
