package org.apache.hadoop.tools.posum.common.records.dataentity;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.hadoop.yarn.util.Records;

import java.io.IOException;

/**
 * Created by ane on 3/29/16.
 */
public class LogEntryDeserializer extends JsonDeserializer<LogEntry> {

    @Override
    public LogEntry deserialize(JsonParser jp, DeserializationContext ctxt)
            throws IOException {
        ObjectNode node = jp.readValueAsTree();
        LogEntry.Type type = LogEntry.Type.valueOf((node.get("type")).asText());
        String id = (node.get("_id")).asText();
        Long timestamp = (node.get("timestamp")).asLong();

        JsonNode detailsNode = node.get("details");
        JsonParser parser = detailsNode.traverse();
        parser.setCodec(jp.getCodec());
        LogEntry entry = Records.newRecord(LogEntry.class);
        entry.setId(id);
        entry.setTimestamp(timestamp);
        entry.setType(type);
        entry.setDetails(parser.readValueAs(type.getDetailsType().getImplClass()));
        return entry;
    }

}
