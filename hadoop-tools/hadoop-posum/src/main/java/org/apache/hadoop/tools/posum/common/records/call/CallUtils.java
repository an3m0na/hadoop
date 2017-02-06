package org.apache.hadoop.tools.posum.common.records.call;

import org.apache.hadoop.tools.posum.common.records.dataentity.LogEntry;
import org.apache.hadoop.tools.posum.common.records.payload.Payload;
import org.apache.hadoop.yarn.util.Records;

public class CallUtils {

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
}
