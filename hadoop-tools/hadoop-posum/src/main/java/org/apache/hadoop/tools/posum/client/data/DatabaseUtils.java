package org.apache.hadoop.tools.posum.client.data;

import org.apache.hadoop.tools.posum.common.records.call.FindByIdCall;
import org.apache.hadoop.tools.posum.common.records.call.StoreLogCall;
import org.apache.hadoop.tools.posum.common.records.dataentity.DatabaseReference;
import org.apache.hadoop.tools.posum.common.records.dataentity.LogEntry;
import org.apache.hadoop.tools.posum.common.records.payload.Payload;
import org.apache.hadoop.tools.posum.common.records.payload.SimplePropertyPayload;
import org.apache.hadoop.tools.posum.common.util.communication.DatabaseProvider;
import org.apache.hadoop.yarn.util.Records;

public class DatabaseUtils {
  public static final String ID_FIELD = "_id";
  public static final DatabaseReference LOG_DB = DatabaseReference.getLogs();

  public static DatabaseProvider newProvider(final Database db) {
    return new DatabaseProvider() {
      @Override
      public Database getDatabase() {
        return db;
      }
    };
  }

  public static <T extends Payload> void storeStatReportCall(LogEntry.Type type, T payload, DataStore dataStore) {
    dataStore.execute(StoreLogCall.newInstance(newStatsLogEntry(type, payload)), LOG_DB);
  }

  public static <T extends Payload> LogEntry<T> findStatsLogEntry(LogEntry.Type type, DataStore dataStore) {
    return dataStore.execute(FindByIdCall.newInstance(type.getCollection(), type.name()), LOG_DB).getEntity();
  }

  public static <T extends Payload> LogEntry<T> newLogEntry(LogEntry.Type type, T details) {
    LogEntry<T> entity = Records.newRecord(LogEntry.class);
    entity.setType(type);
    entity.setDetails(details);
    return entity;
  }

  public static <T extends Payload> void storeLogEntry(LogEntry.Type type, T details, DataStore dataStore) {
    dataStore.execute(StoreLogCall.newInstance(newLogEntry(type, details)), LOG_DB);
  }

  public static <T extends Payload> void storeLogEntry(String message, DataStore dataStore) {
    dataStore.execute(StoreLogCall.newInstance(newLogEntry(message)), LOG_DB);
  }

  public static LogEntry<SimplePropertyPayload> newLogEntry(LogEntry.Type type, String message) {
    return newLogEntry(type == null ? LogEntry.Type.GENERAL : type, SimplePropertyPayload.newInstance(null, message));
  }

  public static LogEntry<SimplePropertyPayload> newLogEntry(String message) {
    return newLogEntry(LogEntry.Type.GENERAL, message);
  }

  public static <T extends Payload> LogEntry<T> newStatsLogEntry(LogEntry.Type type, T payload) {
    LogEntry<T> report = Records.newRecord(LogEntry.class);
    report.setId(type.name());
    report.setType(type);
    report.setDetails(payload);
    return report;
  }
}
