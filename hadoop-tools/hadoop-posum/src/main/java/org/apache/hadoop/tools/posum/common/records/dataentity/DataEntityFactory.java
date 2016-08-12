package org.apache.hadoop.tools.posum.common.records.dataentity;

import org.apache.hadoop.tools.posum.common.records.payload.Payload;
import org.apache.hadoop.yarn.util.Records;

/**
 * Created by ane on 8/12/16.
 */
public class DataEntityFactory {

    //TODO make generic
    public static <T extends Payload> LogEntry<T> getNewLogEntry(LogEntry.Type type, T details) {
        LogEntry<T> entity = Records.newRecord(LogEntry.class);
        entity.setType(type);
        entity.setDetails(details);
        return entity;
    }
}
