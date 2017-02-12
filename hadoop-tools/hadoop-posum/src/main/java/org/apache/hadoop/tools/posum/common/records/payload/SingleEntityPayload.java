package org.apache.hadoop.tools.posum.common.records.payload;

import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.GeneralDataEntity;
import org.apache.hadoop.yarn.util.Records;

public abstract class SingleEntityPayload implements Payload {

  public static SingleEntityPayload newInstance(DataEntityCollection collection, GeneralDataEntity object) {
    SingleEntityPayload payload = Records.newRecord(SingleEntityPayload.class);
    payload.setEntityCollection(collection);
    payload.setEntity(object);
    return payload;
  }

  public abstract DataEntityCollection getEntityCollection();

  public abstract void setEntityCollection(DataEntityCollection type);

  public abstract <T extends GeneralDataEntity> T getEntity();

  public abstract void setEntity(GeneralDataEntity entity);

}
