package org.apache.hadoop.tools.posum.common.records.payload;

import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.GeneralDataEntity;
import org.apache.hadoop.yarn.util.Records;

import java.util.List;

/**
 * Created by ane on 3/20/16.
 */
public abstract class MultiEntityPayload implements Payload{

    public static MultiEntityPayload newInstance(DataEntityCollection type, List<GeneralDataEntity> entities) {
        MultiEntityPayload payload = Records.newRecord(MultiEntityPayload.class);
        payload.setEntityCollection(type);
        payload.setEntities(entities);
        return payload;
    }

    public abstract DataEntityCollection getEntityCollection();

    public abstract void setEntityCollection(DataEntityCollection type);

    public abstract List<GeneralDataEntity> getEntities();

    public abstract void setEntities(List<GeneralDataEntity> entities);
}
