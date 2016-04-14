package org.apache.hadoop.tools.posum.common.records.request;


import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityType;
import org.apache.hadoop.yarn.util.Records;

import java.util.Map;

/**
 * Created by ane on 3/20/16.
 */


public abstract class MultiEntityRequest {

    public static MultiEntityRequest newInstance(DataEntityType type, Map<String, Object>  properties) {
        MultiEntityRequest request = Records.newRecord(MultiEntityRequest.class);
        request.setEntityType(type);
        request.setProperties(properties);
        return request;
    }

    public abstract DataEntityType getEntityType();

    public abstract void setEntityType(DataEntityType type);

    public abstract Map<String, Object> getProperties();

    public abstract void setProperties(Map<String, Object> properties);


}
