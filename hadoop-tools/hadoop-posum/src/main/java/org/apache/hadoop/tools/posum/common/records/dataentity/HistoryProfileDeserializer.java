package org.apache.hadoop.tools.posum.common.records.dataentity;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.hadoop.tools.posum.common.records.dataentity.impl.pb.HistoryProfilePBImpl;

import java.io.IOException;

import static org.apache.hadoop.tools.posum.common.util.Utils.ID_FIELD;

public class HistoryProfileDeserializer extends JsonDeserializer<HistoryProfilePBImpl> {

    @Override
    public HistoryProfilePBImpl deserialize(JsonParser jp, DeserializationContext ctxt)
            throws IOException {
        ObjectNode node = jp.readValueAsTree();
        DataEntityCollection type = DataEntityCollection.valueOf((node.get("type")).asText());
        String id = (node.get(ID_FIELD)).asText();
        Long timestamp = (node.get("timestamp")).asLong();

        JsonNode originalNode = node.get("original");
        JsonParser parser = originalNode.traverse();
        parser.setCodec(jp.getCodec());
        GeneralDataEntity original = parser.readValueAs(type.getMappedClass());
        HistoryProfilePBImpl history = new HistoryProfilePBImpl<>(type, original);
        history.setId(id);
        history.setTimestamp(timestamp);
        return history;
    }

}
