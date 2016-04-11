package org.apache.hadoop.tools.posum.common.records.message.reponse;

import org.apache.hadoop.tools.posum.common.records.dataentity.GeneralDataEntity;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityType;
import org.apache.hadoop.yarn.util.Records;

/**
 * Created by ane on 3/20/16.
 */
public abstract class SingleEntityResponse {

    public static SingleEntityResponse newInstance(DataEntityType type, GeneralDataEntity object) {
        SingleEntityResponse response = Records.newRecord(SingleEntityResponse.class);
        response.setType(type);
        response.setEntity(object);
        return response;
    }

    public abstract DataEntityType getType();

    public abstract void setType(DataEntityType type);

    public abstract GeneralDataEntity getEntity();

    public abstract void setEntity(GeneralDataEntity entity);

}
