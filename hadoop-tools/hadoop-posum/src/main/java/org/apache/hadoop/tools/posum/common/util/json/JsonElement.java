package org.apache.hadoop.tools.posum.common.util.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Created by ane on 5/15/16.
 */
public class JsonElement {

    protected JsonNode innerElement;
    protected static final ObjectMapper mapper = new ObjectMapper();

    public JsonElement() {

    }

    private JsonElement(JsonNode node) {
        innerElement = node;
    }

    public JsonNode getNode() {
        return innerElement;
    }

    public static JsonElement wrapObject(Object object) {
        return new JsonElement(mapper.valueToTree(object));
    }
}
