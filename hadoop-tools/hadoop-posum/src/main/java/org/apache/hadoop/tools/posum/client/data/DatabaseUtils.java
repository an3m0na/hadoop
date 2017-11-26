package org.apache.hadoop.tools.posum.client.data;

import org.apache.hadoop.tools.posum.common.records.call.FindByIdCall;
import org.apache.hadoop.tools.posum.common.records.call.StoreLogCall;
import org.apache.hadoop.tools.posum.common.records.dataentity.LogEntry;
import org.apache.hadoop.tools.posum.common.records.payload.Payload;
import org.apache.hadoop.tools.posum.common.util.communication.DatabaseProvider;
import org.apache.hadoop.yarn.util.Records;

public class DatabaseUtils {
  public static final String ID_FIELD = "_id";

  public static DatabaseProvider newProvider(final Database db) {
    return new DatabaseProvider() {
      @Override
      public Database getDatabase() {
        return db;
      }
    };
  }

  public static <T extends Payload> StoreLogCall storeStatReportCall(LogEntry.Type type, T payload) {
    LogEntry<T> report = Records.newRecord(LogEntry.class);
    report.setId(type.name());
    report.setType(type);
    report.setDetails(payload);
    return StoreLogCall.newInstance(report);
  }

  public static FindByIdCall findStatReportCall(LogEntry.Type type) {
    return FindByIdCall.newInstance(type.getCollection(), type.name());
  }

  public static <T extends Payload> LogEntry<T> newLogEntry(LogEntry.Type type, T details) {
    LogEntry<T> entity = Records.newRecord(LogEntry.class);
    entity.setType(type);
    entity.setDetails(details);
    return entity;
  }
}
