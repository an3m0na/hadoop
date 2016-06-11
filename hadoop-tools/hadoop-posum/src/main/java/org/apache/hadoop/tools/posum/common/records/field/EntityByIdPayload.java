package org.apache.hadoop.tools.posum.common.records.field;

import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityType;
import org.apache.hadoop.yarn.util.Records;

/**
 * Created by ane on 3/20/16.
 */
public abstract class EntityByIdPayload {

    public static EntityByIdPayload newInstance(DataEntityDB db, DataEntityType type, String id) {
        EntityByIdPayload payload = Records.newRecord(EntityByIdPayload.class);
        payload.setEntityDB(db);
        payload.setEntityType(type);
        payload.setId(id);
        return payload;
    }

    public abstract DataEntityDB getEntityDB();

    public abstract void setEntityDB(DataEntityDB db);

    public abstract DataEntityType getEntityType();

    public abstract void setEntityType(DataEntityType type);

    public abstract String getId();

    public abstract void setId(String id);


}
