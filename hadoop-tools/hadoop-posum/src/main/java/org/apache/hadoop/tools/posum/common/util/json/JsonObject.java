package org.apache.hadoop.tools.posum.common.util.json;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;

public class JsonObject extends JsonElement {

  private ObjectNode inner;

  public JsonObject() {
    innerElement = mapper.createObjectNode();
    inner = (ObjectNode) innerElement;
  }

  public JsonObject(ObjectNode node) {
    innerElement = node;
    inner = node;
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

  public JsonElement get(String key) {
    return new JsonElement(inner.get(key));
  }

  public JsonArray getAsArray(String key) {
    ArrayNode array = (ArrayNode) inner.get(key);
    if (array == null)
      return null;
    return new JsonArray(array);
  }

  public static JsonObject readObject(String objectString) throws IOException {
    return new JsonObject((ObjectNode) read(objectString).getNode());
  }

  public Double getNumber(String key) {
    return inner.get(key).asDouble();
  }
}
