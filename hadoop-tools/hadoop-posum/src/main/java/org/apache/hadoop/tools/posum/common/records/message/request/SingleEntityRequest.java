package org.apache.hadoop.tools.posum.common.records.message.request;

import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityType;
import org.apache.hadoop.yarn.util.Records;

/**
 * Created by ane on 3/20/16.
 */
public abstract class SingleEntityRequest {

    public static SingleEntityRequest newInstance(DataEntityType type, String id) {
        SingleEntityRequest request = Records.newRecord(SingleEntityRequest.class);
        request.setType(type);
        request.setId(id);
        return request;
    }

    public abstract DataEntityType getType();

    public abstract void setType(DataEntityType type);

    public abstract String getId();

    public abstract void setId(String id);


}
