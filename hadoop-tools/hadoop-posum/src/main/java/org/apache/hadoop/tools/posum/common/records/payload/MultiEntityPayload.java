package org.apache.hadoop.tools.posum.common.records.payload;

import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.GeneralDataEntity;
import org.apache.hadoop.yarn.util.Records;

import java.util.List;

/**
 * Created by ane on 3/20/16.
 */
public abstract class MultiEntityPayload implements Payload {

    public static <T extends GeneralDataEntity<T>> MultiEntityPayload newInstance(DataEntityCollection type, List<T> entities) {
        MultiEntityPayload payload = Records.newRecord(MultiEntityPayload.class);
        payload.setEntityCollection(type);
        payload.setEntities(entities);
        return payload;
    }

    public abstract DataEntityCollection getEntityCollection();

    public abstract void setEntityCollection(DataEntityCollection type);

    public abstract <T extends GeneralDataEntity<T>> List<T> getEntities();

    public abstract <T extends GeneralDataEntity<T>> void  setEntities(List<T> entities);
}
