package org.apache.hadoop.tools.posum.common.records.field;

import org.apache.hadoop.tools.posum.common.records.dataentity.GeneralDataEntity;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityType;
import org.apache.hadoop.yarn.util.Records;

/**
 * Created by ane on 3/20/16.
 */
public abstract class SingleEntityPayload {

    public static SingleEntityPayload newInstance(DataEntityType type, GeneralDataEntity object) {
        SingleEntityPayload payload = Records.newRecord(SingleEntityPayload.class);
        payload.setEntityType(type);
        payload.setEntity(object);
        return payload;
    }

    public abstract DataEntityType getEntityType();

    public abstract void setEntityType(DataEntityType type);

    public abstract GeneralDataEntity getEntity();

    public abstract void setEntity(GeneralDataEntity entity);

}
