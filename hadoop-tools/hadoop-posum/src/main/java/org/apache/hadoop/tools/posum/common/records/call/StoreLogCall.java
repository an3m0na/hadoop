package org.apache.hadoop.tools.posum.common.records.call;

import org.apache.hadoop.tools.posum.common.records.dataentity.DatabaseReference;
import org.apache.hadoop.tools.posum.common.records.dataentity.LogEntry;
import org.apache.hadoop.tools.posum.common.records.payload.Payload;
import org.apache.hadoop.tools.posum.common.records.payload.SimplePropertyPayload;
import org.apache.hadoop.tools.posum.data.core.LockBasedDataStore;
import org.apache.hadoop.yarn.util.Records;

public abstract class StoreLogCall extends LockBasedDatabaseCallImpl<SimplePropertyPayload> {
  public static StoreLogCall newInstance(LogEntry logEntry) {
    StoreLogCall call = Records.newRecord(StoreLogCall.class);
    call.setLogEntry(logEntry);
    return call;
  }

  public static StoreLogCall newInstance(LogEntry.Type type, String message) {
    return newInstance(CallUtils.newLogEntry(type == null ? LogEntry.Type.GENERAL : type,
      SimplePropertyPayload.newInstance(null, message)));
  }

  public static StoreLogCall newInstance(String message) {
    return newInstance(LogEntry.Type.GENERAL, message);
  }



  public abstract <T extends Payload> LogEntry<T> getLogEntry();

  public abstract void setLogEntry(LogEntry logEntry);


  @Override
  public SimplePropertyPayload execute(LockBasedDataStore dataStore, DatabaseReference db) {
    if (db == null || !db.isOfType(DatabaseReference.Type.SIMULATION)) {
      // do not store unintended logs from simulations
      return SimplePropertyPayload.newInstance("logId",
        dataStore.updateOrStore(DatabaseReference.getLogs(), getLogEntry().getType().getCollection(), getLogEntry()));
    }
    return SimplePropertyPayload.newInstance("logId", null);
  }

  @Override
  protected void lockDatabase(LockBasedDataStore dataStore, DatabaseReference db) {
    if (db == null || !db.isOfType(DatabaseReference.Type.SIMULATION))
      // only lock if it is not a simulation
      dataStore.lockForWrite(DatabaseReference.getLogs());
  }

  @Override
  protected void unlockDatabase(LockBasedDataStore dataStore, DatabaseReference db) {
    if (db == null || !db.isOfType(DatabaseReference.Type.SIMULATION))
      // only unlock if it is not a simulation
      dataStore.unlockForWrite(DatabaseReference.getLogs());
  }
}
