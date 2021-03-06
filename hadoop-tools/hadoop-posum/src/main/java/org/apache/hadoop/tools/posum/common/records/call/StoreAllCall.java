package org.apache.hadoop.tools.posum.common.records.call;

import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.DatabaseReference;
import org.apache.hadoop.tools.posum.common.records.dataentity.GeneralDataEntity;
import org.apache.hadoop.tools.posum.common.records.payload.VoidPayload;
import org.apache.hadoop.tools.posum.data.core.LockBasedDataStore;
import org.apache.hadoop.yarn.util.Records;

import java.util.List;

public abstract class StoreAllCall extends WriteToCollectionCall<VoidPayload> {
  public static StoreAllCall newInstance(DataEntityCollection collection, List<? extends GeneralDataEntity> entities) {
    StoreAllCall call = Records.newRecord(StoreAllCall.class);
    call.setEntityCollection(collection);
    call.setEntities(entities);
    return call;
  }

  public abstract List<? extends GeneralDataEntity> getEntities();

  public abstract void setEntities(List<? extends GeneralDataEntity> entities);

  @Override
  public VoidPayload execute(LockBasedDataStore dataStore, DatabaseReference db) {
    dataStore.storeAll(db, getEntityCollection(), getEntities());
    return VoidPayload.newInstance();
  }

}
