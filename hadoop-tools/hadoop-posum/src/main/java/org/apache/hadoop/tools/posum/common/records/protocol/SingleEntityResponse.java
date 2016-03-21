package org.apache.hadoop.tools.posum.common.records.protocol;

import org.apache.hadoop.tools.posum.common.records.dataentity.AppProfile;
import org.apache.hadoop.tools.posum.database.store.DataEntityType;
import org.apache.hadoop.yarn.util.Records;

/**
 * Created by ane on 3/20/16.
 */
public abstract class SingleEntityResponse {

    public static SingleEntityResponse newInstance(DataEntityType type, AppProfile object) {
        SingleEntityResponse response = Records.newRecord(SingleEntityResponse.class);
        response.setType(type);
        response.setEntity(object);
        return response;
    }


    public abstract DataEntityType getType();

    public abstract void setType(DataEntityType type);

    public abstract AppProfile getEntity();

    public abstract void setEntity(AppProfile entity);

}
