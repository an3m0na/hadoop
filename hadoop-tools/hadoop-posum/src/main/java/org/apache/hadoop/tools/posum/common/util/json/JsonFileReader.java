package org.apache.hadoop.tools.posum.common.util.json;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

public class JsonFileReader {
    private JsonParser jsonParser;

    public JsonFileReader(File file) throws IOException {
        jsonParser = new JsonFactory().createParser(file);
        jsonParser.setCodec(new ObjectMapper());
    }

    public <T> T getNext(Class<T> tClass) throws IOException {
        jsonParser.nextToken();
        if (jsonParser.hasCurrentToken())
            return jsonParser.readValueAs(tClass);
        return null;
    }

    public void close() throws IOException {
        jsonParser.close();
    }

    public <T> T readOne(Class<T> tClass) throws IOException {
        T ret = getNext(tClass);
        jsonParser.close();
        return ret;
    }

    public <T> Iterator<T> readAll(Class<T> tClass) throws IOException {
        Iterator<T> ret = jsonParser.readValuesAs(tClass);
        jsonParser.close();
        return ret;
    }
}
