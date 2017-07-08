package org.apache.hadoop.tools.posum.common.records.payload;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class PolicyInfoMapPayloadDeserializer extends JsonDeserializer<PolicyInfoMapPayload> {

  @Override
  public PolicyInfoMapPayload deserialize(JsonParser jp, DeserializationContext ctxt)
    throws IOException {
    ObjectNode node = jp.readValueAsTree();
    ObjectNode rawEntries = (ObjectNode) node.get("entries");
    Map<String, PolicyInfoPayload> entries = new HashMap<>(rawEntries.size());
    for (Iterator<Map.Entry<String, JsonNode>> iterator = rawEntries.fields(); iterator.hasNext(); ) {
      Map.Entry<String, JsonNode> next = iterator.next();
      JsonParser parser = next.getValue().traverse();
      parser.setCodec(jp.getCodec());
      entries.put(next.getKey(), (PolicyInfoPayload) parser.readValueAs(PayloadType.POLICY_INFO.getImplClass()));
    }
    return PolicyInfoMapPayload.newInstance(entries);
  }

}
