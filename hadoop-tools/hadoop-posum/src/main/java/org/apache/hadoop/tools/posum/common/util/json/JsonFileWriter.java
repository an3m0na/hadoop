package org.apache.hadoop.tools.posum.common.util.json;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;


import java.io.File;
import java.io.IOException;

/**
 * Created by ane on 7/28/16.
 */
public class JsonFileWriter {
    private JsonGenerator jsonGenerator;

    public JsonFileWriter(File file) throws IOException {
        jsonGenerator = new JsonFactory().createGenerator(file, JsonEncoding.UTF8);
        jsonGenerator.setCodec(new ObjectMapper());
    }

    public <T> void write(T object) throws IOException {
        jsonGenerator.writeObject(object);
    }

    public void close() throws IOException {
        jsonGenerator.close();
    }
}
