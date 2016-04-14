package org.apache.hadoop.tools.posum.common.records.response;

import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityType;
import org.apache.hadoop.tools.posum.common.records.dataentity.GeneralDataEntity;
import org.apache.hadoop.yarn.util.Records;

import java.util.List;

/**
 * Created by ane on 3/20/16.
 */
public abstract class MultiEntityPayload {

    public static MultiEntityPayload newInstance(DataEntityType type, List<GeneralDataEntity> entities) {
        MultiEntityPayload response = Records.newRecord(MultiEntityPayload.class);
        response.setEntityType(type);
        response.setEntities(entities);
        return response;
    }

    public abstract DataEntityType getEntityType();

    public abstract void setEntityType(DataEntityType type);

    public abstract List<GeneralDataEntity> getEntities();

    public abstract void setEntities(List<GeneralDataEntity> entities);
}
