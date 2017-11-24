package org.apache.hadoop.tools.posum.common.records.payload;

import org.apache.hadoop.tools.posum.common.records.dataentity.DatabaseReference;
import org.apache.hadoop.yarn.util.Records;

public abstract class DatabaseLockPayload implements Payload {

  public static DatabaseLockPayload newInstance(DatabaseReference db, Long millis) {
    DatabaseLockPayload payload = Records.newRecord(DatabaseLockPayload.class);
    payload.setDatabase(db);
    payload.setMillis(millis);
    return payload;
  }

  public static DatabaseLockPayload newInstance(DatabaseReference db) {
    DatabaseLockPayload payload = Records.newRecord(DatabaseLockPayload.class);
    payload.setDatabase(db);
    return payload;
  }

  public abstract DatabaseReference getDatabase();

  public abstract void setDatabase(DatabaseReference db);

  public abstract Long getMillis();

  public abstract void setMillis(Long millis);
}
