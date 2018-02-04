package org.apache.hadoop.tools.posum.common.util.json;

import com.fasterxml.jackson.databind.node.ArrayNode;

import java.io.IOException;

public class JsonArray extends JsonElement {
  private ArrayNode inner;

  public JsonArray() {
    innerElement = mapper.createArrayNode();
    inner = (ArrayNode) innerElement;
  }

  public JsonArray(ArrayNode inner) {
    this.innerElement = inner;
    this.inner = inner;
  }

  public JsonArray(JsonElement... items) {
    this();
    for (JsonElement item : items) {
      add(item);
    }
  }

  public JsonArray add(JsonElement value) {
    inner.add(value.innerElement);
    return this;
  }

  public JsonArray add(int v) {
    inner.add(v);
    return this;
  }

  public JsonArray add(long v) {
    inner.add(v);
    return this;
  }

  public JsonArray add(float v) {
    inner.add(v);
    return this;
  }

  public JsonArray add(double v) {
    inner.add(v);
    return this;
  }

  public JsonArray add(String v) {
    inner.add(v);
    return this;
  }

  public JsonArray add(boolean v) {
    inner.add(v);
    return this;
  }

  public static JsonArray readArray(String arrayString) throws IOException {
    return new JsonArray((ArrayNode) read(arrayString).getNode());
  }
}
