package org.apache.hadoop.tools.posum.common.records.dataentity;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.hadoop.yarn.util.Records;

import java.io.IOException;

import static org.apache.hadoop.tools.posum.common.util.Utils.ID_FIELD;

public class LogEntryDeserializer extends JsonDeserializer<LogEntry> {

    @Override
    public LogEntry deserialize(JsonParser jp, DeserializationContext ctxt)
            throws IOException {
        ObjectNode node = jp.readValueAsTree();
        LogEntry.Type type = LogEntry.Type.valueOf((node.get("type")).asText());
        String id = (node.get(ID_FIELD)).asText();
        Long timestamp = (node.get("lastUpdated")).asLong();

        JsonNode detailsNode = node.get("details");
        JsonParser parser = detailsNode.traverse();
        parser.setCodec(jp.getCodec());
        LogEntry entry = Records.newRecord(LogEntry.class);
        entry.setId(id);
        entry.setLastUpdated(timestamp);
        entry.setType(type);
        entry.setDetails(parser.readValueAs(type.getDetailsType().getImplClass()));
        return entry;
    }

}
