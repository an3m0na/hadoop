package org.apache.hadoop.tools.posum.common.records.protocol;

import org.apache.hadoop.tools.posum.common.records.profile.AppProfile;
import org.apache.hadoop.tools.posum.database.store.DataCollection;
import org.apache.hadoop.yarn.util.Records;

/**
 * Created by ane on 3/20/16.
 */
public abstract class SingleEntityResponse {

    public static SingleEntityResponse newInstance(DataCollection type, AppProfile object) {
        SingleEntityResponse response = Records.newRecord(SingleEntityResponse.class);
        response.setType(type);
        response.setEntity(object);
        return response;
    }


    public abstract DataCollection getType();

    public abstract void setType(DataCollection type);

    public abstract AppProfile getEntity();

    public abstract void setEntity(AppProfile entity);

}
