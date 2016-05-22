package org.apache.hadoop.tools.posum.common.util;

import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Created by ane on 5/15/16.
 */
public class JsonObject extends JsonElement {

    private ObjectNode inner;

    public JsonObject() {
        innerElement = mapper.createObjectNode();
        inner = (ObjectNode) innerElement;
    }

    public JsonObject put(String property, JsonElement value) {
        inner.put(property, value.innerElement);
        return this;
    }

    public JsonObject put(String s, int v) {
        inner.put(s, v);
        return this;
    }

    public JsonObject put(String s, long v) {
        inner.put(s, v);
        return this;
    }

    public JsonObject put(String s, float v) {
        inner.put(s, v);
        return this;
    }

    public JsonObject put(String s, double v) {
        inner.put(s, v);
        return this;
    }

    public JsonObject put(String s, String v) {
        inner.put(s, v);
        return this;
    }

    public JsonObject put(String s, boolean aBoolean) {
        inner.put(s, aBoolean);
        return this;
    }
}
