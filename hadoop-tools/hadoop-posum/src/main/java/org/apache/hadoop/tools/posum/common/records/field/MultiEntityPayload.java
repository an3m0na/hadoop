package org.apache.hadoop.tools.posum.common.records.field;

import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.GeneralDataEntity;
import org.apache.hadoop.yarn.util.Records;

import java.util.List;

/**
 * Created by ane on 3/20/16.
 */
public abstract class MultiEntityPayload {

    public static MultiEntityPayload newInstance(DataEntityCollection type, List<GeneralDataEntity> entities) {
        MultiEntityPayload payload = Records.newRecord(MultiEntityPayload.class);
        payload.setEntityType(type);
        payload.setEntities(entities);
        return payload;
    }

    public abstract DataEntityCollection getEntityType();

    public abstract void setEntityType(DataEntityCollection type);

    public abstract List<GeneralDataEntity> getEntities();

    public abstract void setEntities(List<GeneralDataEntity> entities);
}
