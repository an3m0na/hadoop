package org.apache.hadoop.tools.posum.common.util.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class JsonElement {

  protected JsonNode innerElement;
  protected static final ObjectMapper mapper = new ObjectMapper();

  public JsonElement() {

  }

  public JsonElement(JsonNode node) {
    innerElement = node;
  }

  public JsonNode getNode() {
    return innerElement;
  }

  public static JsonElement wrapObject(Object object) {
    return new JsonElement(mapper.valueToTree(object));
  }

  public static JsonElement read(String jsonString) throws IOException {
    return new JsonElement(mapper.readTree(jsonString));
  }
}
