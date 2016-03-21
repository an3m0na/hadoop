package org.apache.hadoop.tools.posum.common.records.protocol;

import org.apache.hadoop.tools.posum.common.records.profile.AppProfile;
import org.apache.hadoop.yarn.util.Records;

/**
 * Created by ane on 3/20/16.
 */
public abstract class SingleObjectResponse {

    public static SingleObjectResponse newInstance(AppProfile object) {
        SingleObjectResponse response = Records.newRecord(SingleObjectResponse.class);
        response.setObject(object);
        return response;
    }

    public abstract AppProfile getObject();

    public abstract void setObject(AppProfile object);

}
