package org.apache.hadoop.tools.posum.common.records.payload;

import org.apache.hadoop.yarn.util.Records;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by ane on 3/23/16.
 */
public abstract class SimplePropertyPayload implements Payload, Comparable {

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
        DOUBLE(Double.class) {
            public Object read(String stream) throws IOException {
                return Double.valueOf(stream);
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

    public static SimplePropertyPayload newInstance(String name, Integer value) {
        SimplePropertyPayload property = Records.newRecord(SimplePropertyPayload.class);
        property.setName(name);
        property.setType(PropertyType.INT);
        property.setValue(value);
        return property;
    }

    public static SimplePropertyPayload newInstance(String name, Double value) {
        SimplePropertyPayload property = Records.newRecord(SimplePropertyPayload.class);
        property.setName(name);
        property.setType(PropertyType.DOUBLE);
        property.setValue(value);
        return property;
    }

    public static SimplePropertyPayload newInstance(String name, Long value) {
        SimplePropertyPayload property = Records.newRecord(SimplePropertyPayload.class);
        property.setName(name);
        property.setType(PropertyType.LONG);
        property.setValue(value);
        return property;
    }

    public static SimplePropertyPayload newInstance(String name, Boolean value) {
        SimplePropertyPayload property = Records.newRecord(SimplePropertyPayload.class);
        property.setName(name);
        property.setType(PropertyType.BOOL);
        property.setValue(value);
        return property;
    }

    public static SimplePropertyPayload newInstance(String name, Object value) {
        SimplePropertyPayload property = Records.newRecord(SimplePropertyPayload.class);
        property.setName(name);
        property.setType(PropertyType.STRING);
        property.setValue(value == null ? null : value.toString());
        return property;
    }

    public abstract PropertyType getType();

    public abstract void setType(PropertyType type);

    public abstract Object getValue();

    public abstract void setValue(Object value);

    public abstract String getName();

    public abstract void setName(String name);

    @Override
    public int compareTo(Object o) {
        if (this.equals(o))
            return 0;
        if (o == null || !(o instanceof SimplePropertyPayload))
            return -1;

        SimplePropertyPayload that = (SimplePropertyPayload) o;
        switch (this.getType()) {
            case LONG:
                if (that.getType() == PropertyType.LONG)
                    return ((Long) this.getValue()).compareTo((Long) that.getValue());
                break;
            case DOUBLE:
                if (that.getType() == PropertyType.DOUBLE)
                    return ((Double) this.getValue()).compareTo((Double) that.getValue());
                break;
            case INT:
                if (that.getType() == PropertyType.INT)
                    return ((Integer) this.getValue()).compareTo((Integer) that.getValue());
                break;
        }
        return this.getValue().toString().compareTo(that.getValue().toString());
    }
}
