package org.apache.hadoop.tools.posum.common.records.field;

import org.apache.hadoop.yarn.util.Records;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by ane on 3/23/16.
 */
public abstract class EntityProperty {

    public enum PropertyType {
        STRING(String.class, new ValueReader() {
            @Override
            public Object read(String string) throws IOException {
                return string;
            }
        }),
        LONG(Long.class, new ValueReader() {
            @Override
            public Object read(String stream) throws IOException {
                return Long.valueOf(stream);
            }
        }),
        INT(Integer.class, new ValueReader() {
            @Override
            public Object read(String stream) throws IOException {
                return Integer.valueOf(stream);
            }
        }),
        BOOL(Boolean.class, new ValueReader() {
            @Override
            public Object read(String stream) throws IOException {
                return Boolean.valueOf(stream);
            }
        });


        public interface ValueReader {
            Object read(String stream) throws IOException;
        }

        private Class mappedClass;
        private ValueReader reader;
        private static Map<Class, PropertyType> byClass = new HashMap<>(PropertyType.values().length);

        static {
            for (PropertyType type : PropertyType.values()) {
                byClass.put(type.getMappedClass(), type);
            }
        }

        PropertyType(Class mappedClass, ValueReader reader) {
            this.mappedClass = mappedClass;
            this.reader = reader;
        }

        public Class getMappedClass() {
            return mappedClass;
        }

        public ValueReader getReader() {
            return reader;
        }

        public static PropertyType getByClass(Class type) {
            return byClass.get(type);
        }
    }

    public static EntityProperty newInstance(String name, PropertyType type, Object value) {
        EntityProperty property = Records.newRecord(EntityProperty.class);
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
