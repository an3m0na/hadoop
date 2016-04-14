package org.apache.hadoop.tools.posum.common.records.reponse;

import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityType;
import org.apache.hadoop.tools.posum.common.records.dataentity.GeneralDataEntity;
import org.apache.hadoop.yarn.util.Records;

import java.util.List;

/**
 * Created by ane on 3/20/16.
 */
public abstract class MultiEntityResponse {

    public static MultiEntityResponse newInstance(DataEntityType type, List<GeneralDataEntity> entities) {
        MultiEntityResponse response = Records.newRecord(MultiEntityResponse.class);
        response.setType(type);
        response.setEntities(entities);
        return response;
    }

    public abstract DataEntityType getType();

    public abstract void setType(DataEntityType type);

    public abstract List<GeneralDataEntity> getEntities();

    public abstract void setEntities(List<GeneralDataEntity> entities);
}
