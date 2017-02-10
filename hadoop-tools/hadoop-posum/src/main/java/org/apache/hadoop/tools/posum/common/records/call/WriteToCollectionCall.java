package org.apache.hadoop.tools.posum.common.records.call;

import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.DatabaseReference;
import org.apache.hadoop.tools.posum.common.records.payload.Payload;
import org.apache.hadoop.tools.posum.data.core.LockBasedDataStore;

public abstract class WriteToCollectionCall<T extends Payload> extends LockBasedDatabaseCallImpl<T> {

  public abstract DataEntityCollection getEntityCollection();

  public abstract void setEntityCollection(DataEntityCollection collection);

  @Override
  public void lockDatabase(LockBasedDataStore dataStore, DatabaseReference db) {
    dataStore.lockForWrite(db);
  }

  @Override
  public void unlockDatabase(LockBasedDataStore dataStore, DatabaseReference db) {
    dataStore.unlockForWrite(db);
  }
}
