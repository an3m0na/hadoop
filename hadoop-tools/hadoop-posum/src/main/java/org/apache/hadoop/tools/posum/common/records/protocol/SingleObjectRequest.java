package org.apache.hadoop.tools.posum.common.records.protocol;

import org.apache.hadoop.yarn.util.Records;

/**
 * Created by ane on 3/20/16.
 */
public abstract class SingleObjectRequest {

    public static SingleObjectRequest newInstance(String objectClass, String objectId) {
        SingleObjectRequest request = Records.newRecord(SingleObjectRequest.class);
        request.setObjectClass(objectClass);
        request.setObjectId(objectId);
        return request;
    }

    public abstract String getObjectClass();

    public abstract void setObjectClass(String objectClass);

    public abstract String getObjectId();

    public abstract void setObjectId(String objectId);


}
