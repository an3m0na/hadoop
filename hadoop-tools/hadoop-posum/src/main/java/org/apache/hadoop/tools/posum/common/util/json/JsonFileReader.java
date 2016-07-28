package org.apache.hadoop.tools.posum.common.util.json;

import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

/**
 * Created by ane on 7/28/16.
 */
public class JsonFileReader {
    private final ObjectMapper mapper;
    private JsonParser jsonParser;

    public JsonFileReader(File file) throws IOException {
        mapper = new ObjectMapper();
        jsonParser = mapper.getJsonFactory().createJsonParser(new FileInputStream(file));
    }

    public <T> T getNext(Class<T> tClass) throws IOException {
        try {
            return mapper.readValue(jsonParser, tClass);
        } catch (EOFException e) {
            return null;
        }
    }

    public void close() throws IOException {
        jsonParser.close();
    }

    public <T> T readOne(Class<T> tClass) throws IOException {
        T ret = getNext(tClass);
        jsonParser.close();
        return ret;
    }
}
