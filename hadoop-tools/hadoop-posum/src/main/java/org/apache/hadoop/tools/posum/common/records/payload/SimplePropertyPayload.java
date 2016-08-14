package org.apache.hadoop.tools.posum.common.records.payload;

import org.apache.hadoop.tools.posum.common.util.PosumException;
import org.apache.hadoop.yarn.util.Records;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by ane on 3/23/16.
 */
public abstract class SimplePropertyPayload implements Payload, Comparable {

    public enum PropertyType {
        STRING {
            public Object read(String string) throws IOException {
                return string;
            }
        },
        LONG {
            public Object read(String stream) throws IOException {
                return Long.valueOf(stream);
            }
        },
        DOUBLE {
            public Object read(String stream) throws IOException {
                return Double.valueOf(stream);
            }
        },
        INT {
            public Object read(String stream) throws IOException {
                return Integer.valueOf(stream);
            }
        },
        BOOL {
            public Object read(String stream) throws IOException {
                return Boolean.valueOf(stream);
            }
        },
        ENUM {
            public Object read(String stream) throws IOException {
                int lastDotIndex = stream.lastIndexOf('.');
                try {
                    Class enumClass = Class.forName(stream.substring(0, lastDotIndex));
                    return Enum.valueOf(enumClass, stream.substring(lastDotIndex + 1));
                } catch (ClassNotFoundException e) {
                    throw new PosumException("Could not recognize enum class from property value: " + stream);
                }
            }
        };

        public abstract Object read(String stringValue) throws IOException;
    }

    public static SimplePropertyPayload newInstance(String name, Object value) {
        SimplePropertyPayload property = Records.newRecord(SimplePropertyPayload.class);
        property.setName(name);
        if (value instanceof Enum) {
            property.setType(PropertyType.ENUM);
            property.setValue(value.getClass().getName() + "." + ((Enum) value).name());
        } else {
            if (value instanceof Integer)
                property.setType(PropertyType.INT);
            else if (value instanceof Long)
                property.setType(PropertyType.LONG);
            else if (value instanceof Double)
                property.setType(PropertyType.DOUBLE);
            else if (value instanceof Boolean)
                property.setType(PropertyType.BOOL);
            else property.setType(PropertyType.STRING);
            property.setValue(value == null ? null : value.toString());
        }
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
        Object thisValue = this.getValue();
        Object thatValue = ((SimplePropertyPayload) o).getValue();
        if (thisValue == null)
            return 1;
        if (thatValue == null)
            return -1;
        if (thisValue instanceof Comparable)
            return ((Comparable) thisValue).compareTo(thatValue);
        return this.getValue().toString().compareTo(thatValue.toString());
    }
}
