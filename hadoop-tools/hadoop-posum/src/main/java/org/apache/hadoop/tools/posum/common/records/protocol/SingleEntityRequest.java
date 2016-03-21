package org.apache.hadoop.tools.posum.common.records.protocol;

import org.apache.hadoop.tools.posum.database.store.DataCollection;
import org.apache.hadoop.yarn.util.Records;

/**
 * Created by ane on 3/20/16.
 */
public abstract class SingleEntityRequest {

    public static SingleEntityRequest newInstance(DataCollection type, String id) {
        SingleEntityRequest request = Records.newRecord(SingleEntityRequest.class);
        request.setType(type);
        request.setId(id);
        return request;
    }

    public abstract DataCollection getType();

    public abstract void setType(DataCollection type);

    public abstract String getId();

    public abstract void setId(String id);


}
