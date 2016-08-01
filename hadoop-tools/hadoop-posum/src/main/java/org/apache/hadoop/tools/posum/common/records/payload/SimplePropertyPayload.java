package org.apache.hadoop.tools.posum.common.records.payload;

import org.apache.hadoop.yarn.util.Records;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by ane on 3/23/16.
 */
public abstract class SimplePropertyPayload implements Payload{

    public enum PropertyType {
        STRING(String.class) {
            public Object read(String string) throws IOException {
                return string;
            }
        },
        LONG(Long.class) {
            public Object read(String stream) throws IOException {
                return Long.valueOf(stream);
            }
        },
        INT(Integer.class) {
            public Object read(String stream) throws IOException {
                return Integer.valueOf(stream);
            }
        },
        BOOL(Boolean.class) {
            public Object read(String stream) throws IOException {
                return Boolean.valueOf(stream);
            }
        };

        public interface ValueReader {
            Object read(String stream) throws IOException;
        }

        private Class mappedClass;

        PropertyType(Class mappedClass) {
            this.mappedClass = mappedClass;
        }

        public Class getMappedClass() {
            return mappedClass;
        }

        public abstract Object read(String stringValue) throws IOException;

        public static PropertyType getByClass(Class tClass) {
            for (PropertyType type : values()) {
                if (type.getMappedClass().equals(tClass))
                    return type;
            }
            return null;
        }
    }

    public static SimplePropertyPayload newInstance(String name, PropertyType type, Object value) {
        SimplePropertyPayload property = Records.newRecord(SimplePropertyPayload.class);
        property.setName(name);
        property.setType(type);
        property.setValue(value);
        return property;
    }

    public abstract PropertyType getType();

    public abstract void setType(PropertyType type);

    public abstract Object getValue();

    public abstract void setValue(Object value);

    public abstract String getName();

    public abstract void setName(String name);

}
